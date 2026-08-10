//! Building a runner's VM image from a Dockerfile in the submitted tree.
//!
//! ## Why this is not `docker build`
//!
//! `heyvm mvm build` is a local CLI — `docker build` → `docker export` →
//! `mke2fs`, writing `~/.heyo/images/firecracker/{name}.ext4` on the machine it
//! runs on. It has no remote form, the daemon exposes no image-build route, and
//! `SandboxCreateOptions` has no mounts, so nothing produced inside a sandbox
//! can be written to a host path. An orchestrator that reaches a runner only
//! through the daemon's HTTP API therefore cannot run that pipeline there, and
//! giving a CI job the host access that would let it — write permission on the
//! directory every VM's root filesystem is read from — hands over the runner.
//!
//! What the daemon *does* expose is `POST /sandboxes/{id}/snapshot-image`, which
//! copies a live sandbox's rootfs into that same catalog. So the pipeline here
//! is the one shape that fits the available primitives:
//!
//! ```text
//! FROM  →  create a sandbox from the base image
//! COPY  →  upload the file to its destination through exec, in chunks
//! RUN   →  an exec-operation, with the step timeout the daemon honours
//! ENV   →  a line in /etc/profile.d, because the export discards OCI metadata
//!          anyway and every step runs under `sh -lc`
//!          →  sync, then snapshot-image, then destroy the builder
//! ```
//!
//! The result is a real `.ext4` in the runner's catalog: it outlives the warm
//! VM pool, survives a reboot, and is shared by every later VM built from it.
//! That is the difference between this and folding the Dockerfile into
//! `setup_hooks`, which would re-run the whole install on every cold VM.
//!
//! ## The name is the cache key
//!
//! An image is named `ci-img-<12 hex>`, hashed over the Dockerfile's directives
//! and the bytes of every file in its context. Identical inputs name an image
//! the host already has; any change names one it does not. There is no
//! invalidation step and nothing to remember to bump — "reused until cache
//! busted" is what content addressing does on its own.
//!
//! The daemon refusing to overwrite an existing name is what makes two jobs
//! racing to build the same image safe: the loser is told the name exists,
//! which is exactly the outcome it wanted.
//!
//! ## What is left in the catalog
//!
//! Images are not swept. A rootfs is expensive to rebuild and cheap to keep,
//! and unlike a pooled VM it holds no state from the run that made it — the
//! blunt cleanup is `rm ~/.heyo/images/firecracker/ci-img-*.ext4` on the host,
//! after which the next job rebuilds. `ci_vm_image` is this orchestrator's
//! record of what it has put on each host, and a create that fails because the
//! file went away forgets the row and rebuilds, the same way `acquire_vm`
//! already recovers from a pooled VM the daemon lost.

use crate::vm::{ImageBuild, Vm, VmError, VmSpec};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Hex characters kept from the digest, matching the VM pool's fingerprint.
const FINGERPRINT_LEN: usize = 12;

/// Per-`RUN` ceiling. A cold `apt-get install build-essential` plus a rustup
/// toolchain is minutes; anything past this is a Dockerfile that hangs, and
/// waiting out the job timeout for it would burn the whole budget with no
/// output.
const RUN_TIMEOUT: Duration = Duration::from_secs(45 * 60);

/// How long a build may hold its catalog claim without renewal.
///
/// Longer than the VM lease because it bounds something slower: a whole image
/// build, not a boot. A claim that lapses under a live build only costs a
/// duplicate build, which the daemon's name check then collapses.
pub const BUILD_LEASE: Duration = Duration::from_secs(30 * 60);

// ---- the Dockerfile ----------------------------------------------------

/// One instruction, reduced to what can be replayed inside a booted VM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Directive {
    Run(String),
    Env(String, String),
    Workdir(String),
    /// `COPY <src>… <dest>`. Sources are context-relative; `dest` is a guest path.
    Copy {
        sources: Vec<String>,
        dest: String,
    },
}

/// A parsed Dockerfile: the base image, and the steps to replay on top of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dockerfile {
    pub from: String,
    pub directives: Vec<Directive>,
    /// Instructions that parsed but cannot survive the pipeline, kept so the
    /// build log can say so rather than leaving the author to notice.
    pub ignored: Vec<String>,
}

impl Dockerfile {
    /// Parse the subset that a rootfs build can honour.
    ///
    /// **Unknown instructions are an error, not a shrug.** This file decides
    /// what a build runs, and a silently-dropped `RUN` is a machine that is
    /// missing a dependency for reasons nothing states. The ignorable set is
    /// enumerated rather than open: those are the instructions the heyvm
    /// pipeline genuinely discards.
    pub fn parse(text: &str) -> Result<Self, ImageError> {
        // `docker build` discards OCI metadata on export, so these describe a
        // container image this pipeline never produces. Reported, not obeyed.
        const IGNORABLE: &[&str] = &[
            "CMD",
            "ENTRYPOINT",
            "EXPOSE",
            "LABEL",
            "MAINTAINER",
            "STOPSIGNAL",
            "HEALTHCHECK",
            "VOLUME",
            "SHELL",
        ];

        let mut from: Option<String> = None;
        let mut directives = Vec::new();
        let mut ignored = Vec::new();

        for (lineno, logical) in logical_lines(text) {
            let (verb, rest) = match logical.split_once(char::is_whitespace) {
                Some((v, r)) => (v.to_ascii_uppercase(), r.trim().to_string()),
                None => (logical.to_ascii_uppercase(), String::new()),
            };
            if rest.is_empty() && verb != "USER" {
                return Err(ImageError::Instruction {
                    line: lineno,
                    detail: format!("{verb} needs an argument"),
                });
            }

            match verb.as_str() {
                "FROM" => {
                    if from.is_some() {
                        // Multi-stage needs a builder that can copy between
                        // stages, which a single booted VM is not.
                        return Err(ImageError::Instruction {
                            line: lineno,
                            detail: "a second FROM starts a multi-stage build, which is not \
                                     supported: this builds one VM from one base image"
                                .into(),
                        });
                    }
                    // `AS name` is only meaningful with stages, which is
                    // already refused above.
                    from = Some(
                        rest.split_whitespace()
                            .next()
                            .unwrap_or_default()
                            .to_string(),
                    );
                }
                "RUN" => directives.push(Directive::Run(rest)),
                "WORKDIR" => directives.push(Directive::Workdir(rest)),
                "ENV" => {
                    for (k, v) in parse_env(&rest, lineno)? {
                        directives.push(Directive::Env(k, v));
                    }
                }
                "COPY" | "ADD" => {
                    // `--from=` is a stage reference and `ADD <url>` is a
                    // fetch; both need machinery this does not have, and
                    // guessing at them would produce an image missing files.
                    if rest.starts_with("--") {
                        return Err(ImageError::Instruction {
                            line: lineno,
                            detail: format!(
                                "{verb} flags are not supported; use a plain \
                                             `{verb} <src>… <dest>`"
                            ),
                        });
                    }
                    let mut parts: Vec<String> =
                        rest.split_whitespace().map(str::to_string).collect();
                    if parts.len() < 2 {
                        return Err(ImageError::Instruction {
                            line: lineno,
                            detail: format!("{verb} needs at least one source and a destination"),
                        });
                    }
                    if verb == "ADD" && parts.iter().any(|p| p.contains("://")) {
                        return Err(ImageError::Instruction {
                            line: lineno,
                            detail: "ADD from a URL is not supported; fetch it in a RUN so the \
                                     download is part of the build log"
                                .into(),
                        });
                    }
                    let dest = parts.pop().expect("checked above");
                    directives.push(Directive::Copy {
                        sources: parts,
                        dest,
                    });
                }
                // `ARG` before `FROM` is the common use and it only affects the
                // base image tag, which is spelled out here anyway.
                "ARG" => ignored.push(format!("line {lineno}: ARG is not substituted")),
                v if IGNORABLE.contains(&v) => ignored.push(format!(
                    "line {lineno}: {v} is discarded — the rootfs export keeps no OCI metadata, \
                     and the VM boots through the heyvm init contract"
                )),
                other => {
                    return Err(ImageError::Instruction {
                        line: lineno,
                        detail: format!(
                            "{other} is not supported. This replays a Dockerfile inside a booted \
                             VM, so FROM, RUN, ENV, WORKDIR and COPY are what it can honour."
                        ),
                    });
                }
            }
        }

        let from = from.ok_or(ImageError::NoFrom)?;
        if !directives.iter().any(|d| matches!(d, Directive::Run(_))) && directives.is_empty() {
            return Err(ImageError::Empty);
        }
        Ok(Self {
            from,
            directives,
            ignored,
        })
    }

    /// Every context-relative path this Dockerfile copies in.
    pub fn copied_paths(&self) -> Vec<String> {
        self.directives
            .iter()
            .filter_map(|d| match d {
                Directive::Copy { sources, .. } => Some(sources.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    /// The image name: `ci-img-<12 hex>` over the directives and the context.
    ///
    /// The *parsed* directives rather than the file's bytes, so a reformatted
    /// comment does not rebuild a three-gigabyte image — but every byte of
    /// every copied file, because those are what the image ends up containing.
    /// Sorted by path, so the order `COPY` lines appear in cannot change the
    /// hash of an otherwise identical build.
    pub fn fingerprint(&self, context: &Path, spec: &VmSpec) -> Result<String, ImageError> {
        let mut h = Sha256::new();
        h.update(b"ci-image-v1\0");
        h.update(self.from.as_bytes());
        h.update([0]);
        for d in &self.directives {
            match d {
                Directive::Run(cmd) => {
                    h.update(b"RUN\0");
                    h.update(cmd.as_bytes());
                }
                Directive::Env(k, v) => {
                    h.update(b"ENV\0");
                    h.update(k.as_bytes());
                    h.update([0]);
                    h.update(v.as_bytes());
                }
                Directive::Workdir(w) => {
                    h.update(b"WORKDIR\0");
                    h.update(w.as_bytes());
                }
                Directive::Copy { sources, dest } => {
                    h.update(b"COPY\0");
                    for s in sources {
                        h.update(s.as_bytes());
                        h.update([0]);
                    }
                    h.update(dest.as_bytes());
                }
            }
            h.update([0]);
        }

        // The driver and size decide the rootfs this is snapshotted from, so
        // two spec shapes must not share an image.
        h.update(format!("{:?}\0", spec.driver).as_bytes());

        let mut files = collect_context(context, &self.copied_paths())?;
        files.sort_by(|a, b| a.0.cmp(&b.0));
        for (rel, bytes) in &files {
            h.update(rel.as_bytes());
            h.update([0]);
            let mut fh = Sha256::new();
            fh.update(bytes);
            h.update(fh.finalize());
        }

        Ok(format!(
            "ci-img-{}",
            &hex::encode(h.finalize())[..FINGERPRINT_LEN]
        ))
    }
}

/// Fold continuations and strip comments, keeping the first physical line
/// number of each instruction so an error can point at it.
fn logical_lines(text: &str) -> Vec<(usize, String)> {
    let mut out: Vec<(usize, String)> = Vec::new();
    let mut current = String::new();
    let mut start = 0usize;

    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        // A comment inside a continuation is dropped by docker too.
        if line.starts_with('#') {
            continue;
        }
        if current.is_empty() {
            if line.is_empty() {
                continue;
            }
            start = i + 1;
        }
        if let Some(head) = line.strip_suffix('\\') {
            current.push_str(head.trim_end());
            current.push(' ');
            continue;
        }
        current.push_str(line);
        let done = std::mem::take(&mut current);
        let done = done.trim().to_string();
        if !done.is_empty() {
            out.push((start, done));
        }
    }
    // A trailing backslash with nothing after it.
    let rest = current.trim().to_string();
    if !rest.is_empty() {
        out.push((start, rest));
    }
    out
}

/// `ENV k=v k2=v2` and the legacy `ENV k v`.
fn parse_env(rest: &str, line: usize) -> Result<Vec<(String, String)>, ImageError> {
    if !rest.contains('=') {
        // `ENV key value` — everything after the first space is the value.
        let (k, v) = rest
            .split_once(char::is_whitespace)
            .ok_or(ImageError::Instruction {
                line,
                detail: "ENV needs a name and a value".into(),
            })?;
        return Ok(vec![(k.to_string(), unquote(v.trim()).to_string())]);
    }
    let mut out = Vec::new();
    for pair in split_env_pairs(rest) {
        let (k, v) = pair.split_once('=').ok_or(ImageError::Instruction {
            line,
            detail: format!("ENV entry {pair:?} is not `name=value`"),
        })?;
        if k.is_empty() {
            return Err(ImageError::Instruction {
                line,
                detail: "ENV has an empty name".into(),
            });
        }
        out.push((k.to_string(), unquote(v).to_string()));
    }
    Ok(out)
}

/// Split on whitespace that is not inside quotes, so `ENV A="x y" B=z` is two
/// pairs rather than three tokens.
fn split_env_pairs(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for c in s.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => {
                quote = None;
                cur.push(c);
            }
            (Some(_), c) => cur.push(c),
            (None, '"') | (None, '\'') => {
                quote = Some(c);
                cur.push(c);
            }
            (None, c) if c.is_whitespace() => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            (None, c) => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn unquote(s: &str) -> &str {
    let s = s.trim();
    for q in ['"', '\''] {
        if s.len() >= 2 && s.starts_with(q) && s.ends_with(q) {
            return &s[1..s.len() - 1];
        }
    }
    s
}

/// Read every file a `COPY` names, expanding a directory to its contents.
///
/// Paths are re-checked here even though the workflow validated the build
/// block, because these come out of the Dockerfile rather than the YAML and
/// this function reads files out of a tree somebody submitted.
fn collect_context(
    context: &Path,
    sources: &[String],
) -> Result<Vec<(String, Vec<u8>)>, ImageError> {
    let mut out = Vec::new();
    for src in sources {
        if src.starts_with('/') || src.split('/').any(|s| s == "..") {
            return Err(ImageError::EscapingCopy(src.clone()));
        }
        // `COPY . /dest` is the common spelling for "the whole context".
        let root = if src == "." {
            context.to_path_buf()
        } else {
            context.join(src)
        };
        let meta = std::fs::metadata(&root).map_err(|e| ImageError::MissingContextFile {
            path: src.clone(),
            reason: e.to_string(),
        })?;
        if meta.is_dir() {
            walk(&root, context, &mut out)?;
        } else {
            let rel = rel_of(&root, context);
            let bytes = std::fs::read(&root).map_err(|e| ImageError::MissingContextFile {
                path: src.clone(),
                reason: e.to_string(),
            })?;
            out.push((rel, bytes));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.dedup_by(|a, b| a.0 == b.0);
    Ok(out)
}

fn walk(dir: &Path, context: &Path, out: &mut Vec<(String, Vec<u8>)>) -> Result<(), ImageError> {
    let entries = std::fs::read_dir(dir).map_err(|e| ImageError::MissingContextFile {
        path: rel_of(dir, context),
        reason: e.to_string(),
    })?;
    for entry in entries.flatten() {
        let path = entry.path();
        // Symlinks are not followed: a link out of the context would copy a
        // file the fingerprint never hashed.
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.is_symlink() {
            continue;
        }
        if meta.is_dir() {
            walk(&path, context, out)?;
        } else if meta.is_file() {
            let bytes = std::fs::read(&path).map_err(|e| ImageError::MissingContextFile {
                path: rel_of(&path, context),
                reason: e.to_string(),
            })?;
            out.push((rel_of(&path, context), bytes));
        }
    }
    Ok(())
}

fn rel_of(path: &Path, context: &Path) -> String {
    path.strip_prefix(context)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Resolve a job's `vm.build` against the checkout: parse the Dockerfile and
/// work out what the image would be called.
pub fn plan_for(
    build: &ImageBuild,
    spec: &VmSpec,
    workspace: &Path,
) -> Result<(Dockerfile, PathBuf, String), ImageError> {
    let dockerfile_path = workspace.join(&build.dockerfile);
    let text = std::fs::read_to_string(&dockerfile_path).map_err(|e| ImageError::NoDockerfile {
        path: build.dockerfile.clone(),
        reason: e.to_string(),
    })?;
    let parsed = Dockerfile::parse(&text)?;
    let context = workspace.join(build.context_dir());
    let name = parsed.fingerprint(&context, spec)?;
    Ok((parsed, context, name))
}

// ---- running the build -------------------------------------------------

/// What a build produced, for the log attached to the job.
pub struct Built {
    pub size_bytes: u64,
    pub log: String,
}

/// Replay a Dockerfile inside `vm`, then snapshot it as `name`.
///
/// `vm` is a builder booted from the Dockerfile's `FROM`; it is destroyed by
/// the caller either way. Every step's output is collected into the returned
/// log, because a build that fails is exactly when somebody needs to see what
/// the base image did and did not have.
pub async fn build(
    vm: &Vm,
    parsed: &Dockerfile,
    context: &Path,
    name: &str,
    op_prefix: &str,
) -> Result<Built, ImageError> {
    use std::collections::HashMap;
    use std::fmt::Write as _;

    let mut log = String::new();
    let _ = writeln!(log, "[ci] building image {name} from {}", parsed.from);
    for note in &parsed.ignored {
        let _ = writeln!(log, "[ci] {note}");
    }

    // Carried across directives the way a Dockerfile does: WORKDIR sets the
    // directory for later RUNs, ENV the environment.
    let mut workdir = String::new();
    let mut env: HashMap<String, String> = HashMap::new();
    let mut step = 0usize;

    for directive in &parsed.directives {
        step += 1;
        let op = format!("{op_prefix}.b{step}");
        match directive {
            Directive::Workdir(dir) => {
                workdir = dir.clone();
                let _ = writeln!(log, "\n[ci] WORKDIR {dir}");
                let out = vm
                    .exec(
                        &op,
                        &format!("mkdir -p {}", shell_quote(dir)),
                        &env,
                        Duration::from_secs(60),
                    )
                    .await?;
                if !out.succeeded() {
                    let _ = writeln!(log, "{}", out.combined());
                    return Err(ImageError::Step {
                        step,
                        what: format!("WORKDIR {dir}"),
                        detail: out.combined(),
                    });
                }
            }
            Directive::Env(k, v) => {
                env.insert(k.clone(), v.clone());
                let _ = writeln!(log, "\n[ci] ENV {k}={v}");
                // Written to /etc/profile.d as well as held in `env`: the
                // export discards OCI metadata, so a variable that only lived
                // in this process would be absent from every step of every job
                // that later runs on the image. The daemon renders a command as
                // `sh -lc`, which reads profile.d — the same trick
                // deploy/image/Dockerfile uses by hand.
                let line = format!("export {}={}", k, shell_quote(v));
                let script = format!(
                    "mkdir -p /etc/profile.d && printf '%s\\n' {} >> /etc/profile.d/10-ci-image.sh \
                     && chmod 644 /etc/profile.d/10-ci-image.sh",
                    shell_quote(&line)
                );
                let out = vm.exec(&op, &script, &env, Duration::from_secs(60)).await?;
                if !out.succeeded() {
                    return Err(ImageError::Step {
                        step,
                        what: format!("ENV {k}"),
                        detail: out.combined(),
                    });
                }
            }
            Directive::Copy { sources, dest } => {
                let _ = writeln!(log, "\n[ci] COPY {} {dest}", sources.join(" "));
                let files = collect_context(context, sources)?;
                for (i, (rel, bytes)) in files.iter().enumerate() {
                    // A single source that is a file copies *to* `dest`; a
                    // directory or several sources copy *into* it, as docker
                    // does.
                    let target = if files.len() == 1 && !dest.ends_with('/') {
                        dest.clone()
                    } else {
                        format!("{}/{}", dest.trim_end_matches('/'), rel)
                    };
                    vm.upload_bytes(&format!("{op}.f{i}"), &target, bytes)
                        .await?;
                    let _ = writeln!(log, "  {rel} → {target} ({} bytes)", bytes.len());
                }
            }
            Directive::Run(cmd) => {
                let _ = writeln!(log, "\n[ci] RUN {cmd}");
                let script = if workdir.is_empty() {
                    cmd.clone()
                } else {
                    format!("cd {} || exit 1\n{cmd}", shell_quote(&workdir))
                };
                let out = vm.exec(&op, &script, &env, RUN_TIMEOUT).await?;
                let _ = writeln!(log, "{}", out.combined());
                if !out.succeeded() {
                    return Err(ImageError::Step {
                        step,
                        what: format!("RUN {cmd}"),
                        detail: format!("exited {}", out.exit_code),
                    });
                }
            }
        }
    }

    // The daemon copies the live rootfs without pausing the VM, so anything
    // still in page cache would be missing from the image. This is the
    // quiesce its documentation asks the caller for.
    let _ = writeln!(log, "\n[ci] sync");
    let out = vm
        .exec(
            &format!("{op_prefix}.sync"),
            "sync; sleep 1; sync",
            &env,
            Duration::from_secs(300),
        )
        .await?;
    if !out.succeeded() {
        return Err(ImageError::Step {
            step: step + 1,
            what: "sync".into(),
            detail: out.combined(),
        });
    }

    let _ = writeln!(log, "[ci] snapshotting the rootfs as {name}");
    let size_bytes = match vm.snapshot_to_image(name).await {
        Ok(size) => size,
        // Another job built the identical image while this one was working.
        // Both wanted the same bytes under the same name, so the race has no
        // loser — this one just did the work for nothing.
        Err(VmError::ImageExists(_)) => {
            let _ = writeln!(
                log,
                "[ci] another build finished this image first; using theirs"
            );
            0
        }
        Err(e) => return Err(ImageError::Vm(e)),
    };

    let _ = writeln!(log, "[ci] image {name} is ready");
    Ok(Built { size_bytes, log })
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

// ---- the catalog -------------------------------------------------------

/// One image this orchestrator has built, or is building, on one runner.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub name: String,
    pub runner_hd_id: String,
    pub status: String,
    pub workflow_id: String,
    pub built_by_job: Option<String>,
    pub size_bytes: i64,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub ready_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl CatalogEntry {
    fn from_row(r: &sqlx::postgres::PgRow) -> Self {
        Self {
            name: r.get("name"),
            runner_hd_id: r.get("runner_hd_id"),
            status: r.get("status"),
            workflow_id: r.get("workflow_id"),
            built_by_job: r.get("built_by_job"),
            size_bytes: r.get("size_bytes"),
            error: r.get("error"),
            created_at: r.get("created_at"),
            ready_at: r.get("ready_at"),
        }
    }
}

/// What this orchestrator has put in each runner's image catalog.
///
/// The daemon has no route to list its images — `heyvm mvm images` reads the
/// directory locally — so "does this host already have it" cannot be asked over
/// the tunnel. This table answers it instead, on the same reasoning
/// `ci_vm_pool` is the source of truth for VMs: a record kept here is one query,
/// and drift is self-healing because a create against a missing image fails and
/// forgets the row.
#[derive(Clone)]
pub struct Catalog {
    db: PgPool,
}

/// What [`Catalog::claim`] found.
pub enum Claim {
    /// The runner has it. Use it.
    Ready,
    /// Nobody is building it and this caller now owns doing so.
    Build,
    /// Somebody else is building it; wait rather than build a second copy.
    InProgress,
}

impl Catalog {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    /// Decide, in one statement, whether to use, build, or wait.
    ///
    /// The insert is what makes it a decision rather than a read: two jobs
    /// racing arrive at the same primary key, and exactly one of them inserts.
    /// The other is told `InProgress` and waits — which is the difference
    /// between one image build on a host and one per concurrent job.
    ///
    /// A claim whose lease has lapsed is taken over rather than waited on, so a
    /// dispatcher that died mid-build does not block the image for ever.
    pub async fn claim(
        &self,
        name: &str,
        runner: &str,
        workflow_id: &str,
        job_id: &str,
        lease: Duration,
    ) -> Result<Claim, ImageError> {
        let row = sqlx::query(
            "INSERT INTO ci_vm_image
                (name, runner_hd_id, workflow_id, status, built_by_job, leased_until)
             VALUES ($1,$2,$3,'building',$4, now() + make_interval(secs => $5))
             ON CONFLICT (name, runner_hd_id) DO UPDATE
                SET status='building', built_by_job=$4, error=NULL,
                    leased_until=now() + make_interval(secs => $5)
              WHERE ci_vm_image.status <> 'ready'
                AND (ci_vm_image.leased_until IS NULL OR ci_vm_image.leased_until < now())
             RETURNING status",
        )
        .bind(name)
        .bind(runner)
        .bind(workflow_id)
        .bind(job_id)
        .bind(lease.as_secs() as f64)
        .fetch_optional(&self.db)
        .await
        .map_err(ImageError::sql)?;

        if row.is_some() {
            return Ok(Claim::Build);
        }
        // The upsert did nothing, so a row exists that it was not allowed to
        // take: either it is ready, or somebody's lease is still live.
        match self.status_of(name, runner).await? {
            Some(s) if s == "ready" => Ok(Claim::Ready),
            _ => Ok(Claim::InProgress),
        }
    }

    pub async fn status_of(&self, name: &str, runner: &str) -> Result<Option<String>, ImageError> {
        let row =
            sqlx::query("SELECT status FROM ci_vm_image WHERE name = $1 AND runner_hd_id = $2")
                .bind(name)
                .bind(runner)
                .fetch_optional(&self.db)
                .await
                .map_err(ImageError::sql)?;
        Ok(row.map(|r| r.get("status")))
    }

    /// Hold a claim while a long build runs.
    pub async fn renew(&self, name: &str, runner: &str, lease: Duration) -> Result<(), ImageError> {
        sqlx::query(
            "UPDATE ci_vm_image SET leased_until = now() + make_interval(secs => $3)
              WHERE name = $1 AND runner_hd_id = $2 AND status = 'building'",
        )
        .bind(name)
        .bind(runner)
        .bind(lease.as_secs() as f64)
        .execute(&self.db)
        .await
        .map_err(ImageError::sql)?;
        Ok(())
    }

    pub async fn mark_ready(
        &self,
        name: &str,
        runner: &str,
        size_bytes: u64,
    ) -> Result<(), ImageError> {
        sqlx::query(
            "UPDATE ci_vm_image
                SET status='ready', ready_at=now(), leased_until=NULL, error=NULL,
                    size_bytes=$3
              WHERE name = $1 AND runner_hd_id = $2",
        )
        .bind(name)
        .bind(runner)
        .bind(size_bytes as i64)
        .execute(&self.db)
        .await
        .map_err(ImageError::sql)?;
        Ok(())
    }

    /// Record a failed build, keeping the row so the page can say what happened.
    ///
    /// `failed` rather than deleted, and unleased: the next job to want this
    /// image takes the claim and tries again, which is right for a build that
    /// failed on a transient apt mirror — while the row still carries the
    /// reason the last attempt gave.
    pub async fn mark_failed(
        &self,
        name: &str,
        runner: &str,
        error: &str,
    ) -> Result<(), ImageError> {
        sqlx::query(
            "UPDATE ci_vm_image
                SET status='failed', leased_until=NULL, error=$3
              WHERE name = $1 AND runner_hd_id = $2 AND status <> 'ready'",
        )
        .bind(name)
        .bind(runner)
        .bind(error)
        .execute(&self.db)
        .await
        .map_err(ImageError::sql)?;
        Ok(())
    }

    /// Drop the record of an image the runner turns out not to have.
    pub async fn forget(&self, name: &str, runner: &str) -> Result<(), ImageError> {
        sqlx::query("DELETE FROM ci_vm_image WHERE name = $1 AND runner_hd_id = $2")
            .bind(name)
            .bind(runner)
            .execute(&self.db)
            .await
            .map_err(ImageError::sql)?;
        Ok(())
    }

    /// Every image on the runners this instance serves.
    pub async fn inventory(&self, runners: &[String]) -> Result<Vec<CatalogEntry>, ImageError> {
        if runners.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT * FROM ci_vm_image
              WHERE runner_hd_id = ANY($1)
              ORDER BY runner_hd_id, created_at DESC",
        )
        .bind(runners)
        .fetch_all(&self.db)
        .await
        .map_err(ImageError::sql)?;
        Ok(rows.iter().map(CatalogEntry::from_row).collect())
    }
}

#[derive(Debug)]
pub enum ImageError {
    NoDockerfile {
        path: String,
        reason: String,
    },
    Instruction {
        line: usize,
        detail: String,
    },
    NoFrom,
    Empty,
    EscapingCopy(String),
    MissingContextFile {
        path: String,
        reason: String,
    },
    Step {
        step: usize,
        what: String,
        detail: String,
    },
    Vm(VmError),
    Sql(String),
    /// Somebody else's build did not finish inside the window this job could
    /// wait for it.
    WaitTimeout {
        name: String,
        waited: Duration,
    },
}

impl ImageError {
    fn sql(e: sqlx::Error) -> Self {
        Self::Sql(e.to_string())
    }
}

impl From<VmError> for ImageError {
    fn from(e: VmError) -> Self {
        Self::Vm(e)
    }
}

impl fmt::Display for ImageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDockerfile { path, reason } => write!(
                f,
                "vm.build.dockerfile {path:?} could not be read from the submitted tree: {reason}"
            ),
            Self::Instruction { line, detail } => {
                write!(f, "Dockerfile line {line}: {detail}")
            }
            Self::NoFrom => write!(f, "the Dockerfile has no FROM, so there is no base image"),
            Self::Empty => write!(f, "the Dockerfile has no instructions to run"),
            Self::EscapingCopy(p) => write!(
                f,
                "COPY source {p:?} escapes the build context; it must be a relative path \
                 with no `..` segments"
            ),
            Self::MissingContextFile { path, reason } => {
                write!(f, "COPY source {path:?} could not be read: {reason}")
            }
            Self::Step { step, what, detail } => write!(
                f,
                "image build failed at instruction {step} ({what}): {detail}"
            ),
            Self::Vm(e) => write!(f, "{e}"),
            Self::Sql(e) => write!(f, "database error: {e}"),
            Self::WaitTimeout { name, waited } => write!(
                f,
                "another job has been building image {name} on this runner for {waited:?} and \
                 has not finished. This job gave up waiting rather than building a second copy; \
                 it will be retried."
            ),
        }
    }
}

impl std::error::Error for ImageError {}

#[cfg(test)]
mod tests {
    use super::*;
    use heyo_sdk::{SandboxDriver, SandboxSize};
    use std::collections::BTreeMap;

    fn spec() -> VmSpec {
        VmSpec {
            driver: SandboxDriver::Firecracker,
            image: None,
            build: None,
            size_class: Some(SandboxSize::Medium),
            disk_size_gb: Some(20),
            working_directory: None,
            env_vars: BTreeMap::new(),
            setup_hooks: vec![],
            cache_key_files: vec![],
            reuse: true,
            ttl_seconds: None,
        }
    }

    #[test]
    fn a_dockerfile_parses_into_replayable_directives() {
        let d = Dockerfile::parse(
            "# a comment\n\
             FROM ubuntu:24.04\n\
             RUN apt-get update \\\n    && apt-get install -y curl\n\
             ENV A=1 B=\"two words\"\n\
             WORKDIR /src\n\
             COPY init.sh /init.sh\n\
             EXPOSE 22\n\
             CMD [\"/init.sh\"]\n",
        )
        .unwrap();

        assert_eq!(d.from, "ubuntu:24.04");
        assert_eq!(
            d.directives,
            vec![
                Directive::Run("apt-get update && apt-get install -y curl".into()),
                Directive::Env("A".into(), "1".into()),
                Directive::Env("B".into(), "two words".into()),
                Directive::Workdir("/src".into()),
                Directive::Copy {
                    sources: vec!["init.sh".into()],
                    dest: "/init.sh".into()
                },
            ]
        );
        // Discarded by the pipeline, but said out loud.
        assert_eq!(d.ignored.len(), 2, "{:?}", d.ignored);
        assert!(d.ignored.iter().any(|n| n.contains("EXPOSE")));
        assert!(d.ignored.iter().any(|n| n.contains("CMD")));
    }

    /// The house rule everywhere else in this codebase: an instruction nobody
    /// implemented must not look like one that worked.
    #[test]
    fn an_unsupported_instruction_is_refused_rather_than_dropped() {
        let e = Dockerfile::parse("FROM ubuntu\nONBUILD RUN true\n").unwrap_err();
        assert!(e.to_string().contains("ONBUILD"), "{e}");
        assert!(e.to_string().contains("line 2"), "{e}");

        // A second FROM is multi-stage, which one booted VM cannot express.
        let e = Dockerfile::parse("FROM a\nRUN true\nFROM b\n").unwrap_err();
        assert!(e.to_string().contains("multi-stage"), "{e}");

        // A stage reference or a URL fetch would silently produce an image
        // missing files.
        let e = Dockerfile::parse("FROM a\nCOPY --from=build /x /y\n").unwrap_err();
        assert!(e.to_string().contains("flags are not supported"), "{e}");
        let e = Dockerfile::parse("FROM a\nADD https://x/y.tar /y\n").unwrap_err();
        assert!(e.to_string().contains("URL"), "{e}");

        assert!(matches!(
            Dockerfile::parse("RUN true\n").unwrap_err(),
            ImageError::NoFrom
        ));
    }

    #[test]
    fn env_parses_both_spellings_and_keeps_quoted_spaces() {
        let d = Dockerfile::parse("FROM a\nENV LEGACY some value here\n").unwrap();
        assert_eq!(
            d.directives,
            vec![Directive::Env("LEGACY".into(), "some value here".into())]
        );

        let d = Dockerfile::parse("FROM a\nENV A=1 B='x y' C=\"z\"\n").unwrap();
        assert_eq!(
            d.directives,
            vec![
                Directive::Env("A".into(), "1".into()),
                Directive::Env("B".into(), "x y".into()),
                Directive::Env("C".into(), "z".into()),
            ]
        );
    }

    fn ctx(files: &[(&str, &str)]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ci-img-{}", crate::vm::new_id()));
        for (rel, body) in files {
            let p = dir.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, body).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The whole cache story in one test: the name is the content, so an
    /// unchanged Dockerfile reuses and any change rebuilds.
    #[test]
    fn the_image_name_is_the_hash_of_the_dockerfile_and_its_context() {
        let text = "FROM ubuntu:24.04\nCOPY init.sh /init.sh\nRUN chmod +x /init.sh\n";
        let d = Dockerfile::parse(text).unwrap();
        let c = ctx(&[("init.sh", "#!/bin/sh\necho hi\n")]);

        let first = d.fingerprint(&c, &spec()).unwrap();
        assert!(first.starts_with("ci-img-"), "{first}");
        assert_eq!(first.len(), "ci-img-".len() + FINGERPRINT_LEN);
        assert_eq!(
            first,
            d.fingerprint(&c, &spec()).unwrap(),
            "the same inputs must name the same image, or nothing is ever reused"
        );

        // A copied file changing is a different image.
        std::fs::write(c.join("init.sh"), "#!/bin/sh\necho changed\n").unwrap();
        let after_file = d.fingerprint(&c, &spec()).unwrap();
        assert_ne!(
            first, after_file,
            "a changed COPY source must bust the cache"
        );

        // So is an instruction changing.
        let d2 = Dockerfile::parse(
            "FROM ubuntu:24.04\nCOPY init.sh /init.sh\nRUN chmod +x /init.sh && true\n",
        )
        .unwrap();
        assert_ne!(after_file, d2.fingerprint(&c, &spec()).unwrap());

        // And so is the base image.
        let d3 = Dockerfile::parse(text.replace("24.04", "22.04").as_str()).unwrap();
        assert_ne!(after_file, d3.fingerprint(&c, &spec()).unwrap());

        // A comment is not.
        let d4 = Dockerfile::parse(&format!("# hello\n{text}")).unwrap();
        assert_eq!(
            after_file,
            d4.fingerprint(&c, &spec()).unwrap(),
            "reformatting must not rebuild a multi-gigabyte image"
        );

        std::fs::remove_dir_all(&c).ok();
    }

    /// The build reads files out of a tree somebody submitted, so this is the
    /// same rule `cache_key_files` has and for the same reason.
    #[test]
    fn a_copy_cannot_escape_the_context() {
        let c = ctx(&[("ok.txt", "x")]);
        for bad in ["../secrets", "/etc/passwd", "a/../../b"] {
            let d = Dockerfile::parse(&format!("FROM a\nCOPY {bad} /dest\n")).unwrap();
            assert!(
                matches!(d.fingerprint(&c, &spec()), Err(ImageError::EscapingCopy(_))),
                "{bad} must be refused"
            );
        }
        std::fs::remove_dir_all(&c).ok();
    }

    #[test]
    fn a_directory_source_hashes_every_file_under_it() {
        let c = ctx(&[("app/a.txt", "one"), ("app/nested/b.txt", "two")]);
        let d = Dockerfile::parse("FROM a\nCOPY app /app\n").unwrap();
        let before = d.fingerprint(&c, &spec()).unwrap();

        std::fs::write(c.join("app/nested/b.txt"), "changed").unwrap();
        assert_ne!(
            before,
            d.fingerprint(&c, &spec()).unwrap(),
            "a file deep in a copied directory still busts the cache"
        );
        std::fs::remove_dir_all(&c).ok();
    }

    #[test]
    fn a_missing_copy_source_names_the_path() {
        let c = ctx(&[("present", "x")]);
        let d = Dockerfile::parse("FROM a\nCOPY absent /dest\n").unwrap();
        let e = d.fingerprint(&c, &spec()).unwrap_err();
        assert!(e.to_string().contains("absent"), "{e}");
        std::fs::remove_dir_all(&c).ok();
    }

    /// `heyvm mvm build -f x/Dockerfile` defaults its context to `x`, and a
    /// workflow that says only `dockerfile:` should mean the same thing.
    #[test]
    fn the_context_defaults_to_the_dockerfiles_directory() {
        let b = ImageBuild {
            dockerfile: "deploy/image/Dockerfile".into(),
            context: None,
        };
        assert_eq!(b.context_dir(), "deploy/image");
        let b = ImageBuild {
            dockerfile: "Dockerfile".into(),
            context: None,
        };
        assert_eq!(b.context_dir(), ".");
        let b = ImageBuild {
            dockerfile: "deploy/image/Dockerfile".into(),
            context: Some("deploy".into()),
        };
        assert_eq!(b.context_dir(), "deploy");
    }

    // ---- the catalog ----------------------------------------------------
    //
    //   CI_TEST_DATABASE_URL=... cargo test -- --ignored image::

    async fn test_catalog() -> Catalog {
        let url = std::env::var("CI_TEST_DATABASE_URL").expect("CI_TEST_DATABASE_URL");
        let dir = std::env::temp_dir().join(format!("ci-img-logs-{}", crate::vm::new_id()));
        let store = crate::store::Store::connect(&url, dir).await.unwrap();
        store
            .migrate(Path::new("migrations"))
            .await
            .expect("migrations");
        Catalog::new(store.pool().clone())
    }

    /// A distinct runner per test so concurrent runs never contend.
    fn runner_id() -> String {
        format!("hd-{}", crate::vm::new_id().replace('-', ""))
    }

    const LEASE: Duration = Duration::from_secs(600);
    const LAPSED: Duration = Duration::from_secs(0);

    /// The whole point of the table: one build per host, and every later job
    /// finds it ready instead of rebuilding a multi-gigabyte rootfs.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn one_job_builds_an_image_and_the_rest_wait_then_reuse_it() {
        let c = test_catalog().await;
        let runner = runner_id();
        let name = "ci-img-aaaaaaaaaaaa";

        assert!(matches!(
            c.claim(name, &runner, "wf", "job-1", LEASE).await.unwrap(),
            Claim::Build
        ));
        // A second job arriving mid-build must not start its own.
        assert!(matches!(
            c.claim(name, &runner, "wf", "job-2", LEASE).await.unwrap(),
            Claim::InProgress
        ));

        c.mark_ready(name, &runner, 4096).await.unwrap();
        for job in ["job-2", "job-3"] {
            assert!(
                matches!(
                    c.claim(name, &runner, "wf", job, LEASE).await.unwrap(),
                    Claim::Ready
                ),
                "{job} must reuse the image rather than rebuild it"
            );
        }

        // A ready image is never taken over, however old its row.
        assert!(matches!(
            c.claim(name, &runner, "wf", "job-4", LAPSED).await.unwrap(),
            Claim::Ready
        ));

        let seen = c.inventory(std::slice::from_ref(&runner)).await.unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].status, "ready");
        assert_eq!(seen[0].size_bytes, 4096);
        c.forget(name, &runner).await.unwrap();
    }

    /// An image is a file on one host's disk, so one runner having it says
    /// nothing about another.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn an_image_on_one_runner_is_not_an_image_on_another() {
        let c = test_catalog().await;
        let (a, b) = (runner_id(), runner_id());
        let name = "ci-img-bbbbbbbbbbbb";

        c.claim(name, &a, "wf", "job-1", LEASE).await.unwrap();
        c.mark_ready(name, &a, 1).await.unwrap();

        assert!(matches!(
            c.claim(name, &b, "wf", "job-2", LEASE).await.unwrap(),
            Claim::Build,
        ));
        assert_eq!(c.inventory(&[a.clone()]).await.unwrap().len(), 1);
        c.forget(name, &a).await.unwrap();
        c.forget(name, &b).await.unwrap();
    }

    /// A dispatcher that died mid-build must not block the image for ever, and
    /// a failed build must be retried by the next job that wants it — with the
    /// last reason still on the row for whoever looks.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn a_lapsed_or_failed_build_is_taken_over_by_the_next_job() {
        let c = test_catalog().await;
        let runner = runner_id();
        let name = "ci-img-cccccccccccc";

        // A holder that stopped renewing.
        c.claim(name, &runner, "wf", "dead-job", LAPSED)
            .await
            .unwrap();
        assert!(matches!(
            c.claim(name, &runner, "wf", "job-2", LEASE).await.unwrap(),
            Claim::Build,
        ));
        // And renewing keeps it held against a third.
        c.renew(name, &runner, LEASE).await.unwrap();
        assert!(matches!(
            c.claim(name, &runner, "wf", "job-3", LEASE).await.unwrap(),
            Claim::InProgress
        ));

        c.mark_failed(name, &runner, "apt-get exited 100")
            .await
            .unwrap();
        let seen = c.inventory(std::slice::from_ref(&runner)).await.unwrap();
        assert_eq!(seen[0].status, "failed");
        assert_eq!(seen[0].error.as_deref(), Some("apt-get exited 100"));

        // Retried rather than stuck: a mirror that was down is worth another go.
        assert!(matches!(
            c.claim(name, &runner, "wf", "job-4", LEASE).await.unwrap(),
            Claim::Build
        ));
        c.forget(name, &runner).await.unwrap();
        assert!(c.inventory(&[runner]).await.unwrap().is_empty());
    }

    /// This repository's own image, which is the worked example the README
    /// points at — it has to survive the parser.
    #[test]
    fn this_repositorys_own_dockerfile_parses() {
        let text = include_str!("../deploy/image/Dockerfile");
        let d = Dockerfile::parse(text).expect("deploy/image/Dockerfile parses");
        assert_eq!(d.from, "ubuntu:24.04");
        assert!(
            d.directives
                .iter()
                .filter(|d| matches!(d, Directive::Run(_)))
                .count()
                >= 5
        );
        assert!(
            d.directives.contains(&Directive::Copy {
                sources: vec!["init.sh".into()],
                dest: "/init.sh".into()
            }),
            "{:?}",
            d.directives
        );
        // ENV RUSTUP_HOME/CARGO_HOME must survive as directives — the whole
        // reason the hand-written image needs its profile.d hack.
        assert!(
            d.directives
                .iter()
                .any(|x| matches!(x, Directive::Env(k, _) if k == "CARGO_HOME"))
        );
    }
}
