//! The submit endpoint: `git submit` posts here.
//!
//! The client sends a **`git archive` tarball of the tree it wants built**,
//! signed with HMAC-SHA256. Two consequences follow from that choice, and both
//! are the point:
//!
//! - **No repository credential exists anywhere in this system.** Not on the
//!   orchestrator, not in a guest. The submitter already had read access — they
//!   ran `git archive` — so nothing here needs its own. A CI system that clones
//!   for you is a CI system holding a key to every repository it builds.
//! - **The tree is exactly what the submitter meant.** No re-resolving a ref
//!   that may have moved, no guessing whether `--dirty` work was included. What
//!   arrives is what runs.
//!
//! The cost is that the guest gets a tree with no `.git`, so `git describe` and
//! friends do not work in a step. The commit, ref and branch are supplied as
//! `CI_*`/`GITHUB_*` environment variables instead, which covers what build
//! scripts actually read them for.
//!
//! ## The signature is the whole security boundary
//!
//! This route is in the deployment's `public_paths`, because an app-lb gate
//! admits browsers only and `git submit` is not a browser. So a credential here is
//! stands between the open internet and arbitrary code execution on a runner.
//! It is compared in constant time, over the raw body, before the body is
//! parsed — a JSON parse on unauthenticated input is a decision, not a default.

use crate::config::Config;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use subtle::ConstantTimeEq;

/// Header carrying the signature, matching what `git submit` already sends.
pub const SIGNATURE_HEADER: &str = "x-heyo-signature-256";
/// Header carrying the client's version, for diagnosing a stale client.
pub const VERSION_HEADER: &str = "x-heyo-git-ci-version";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryRef {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub default_branch: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitIdentity {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

/// The submitted tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceArchive {
    /// Only `tar.gz` today. Named rather than assumed so a future format is a
    /// rejected value instead of a corrupt extraction.
    pub format: String,
    pub content_base64: String,
}

/// What `git submit` posts.
///
/// Field names follow the existing `git submit` payload so the two clients stay
/// recognisably related.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitRequest {
    #[serde(default)]
    pub repository: RepositoryRef,
    #[serde(default)]
    pub r#ref: String,
    #[serde(default)]
    pub before: String,
    #[serde(default)]
    pub after: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub pusher: Option<GitIdentity>,
    /// Which workflow object this run belongs to. Absent means "every workflow
    /// object whose repository matches", resolved by the caller.
    #[serde(default)]
    pub workflow_id: Option<String>,
    pub source: SourceArchive,
}

impl SubmitRequest {
    /// The branch name, with `refs/heads/` stripped.
    pub fn branch(&self) -> &str {
        self.r#ref
            .strip_prefix("refs/heads/")
            .unwrap_or(&self.r#ref)
    }
}

/// Verify `x-heyo-signature-256` over the raw body.
///
/// The header is `sha256=<hex>`, matching `git submit` and GitHub's webhook
/// convention. Compared with `subtle`'s constant-time equality: a byte-wise `==`
/// returns as soon as it finds a difference, which leaks the correct prefix one
/// request at a time.
pub fn verify_signature(
    secret: &str,
    body: &[u8],
    header: Option<&str>,
) -> Result<(), TriggerError> {
    let Some(header) = header else {
        return Err(TriggerError::MissingSignature);
    };
    let hex_sig = header
        .trim()
        .strip_prefix("sha256=")
        .ok_or(TriggerError::MalformedSignature)?;
    let provided = hex::decode(hex_sig).map_err(|_| TriggerError::MalformedSignature)?;

    let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes())
        .map_err(|_| TriggerError::MalformedSignature)?;
    mac.update(body);
    let expected = mac.finalize().into_bytes();

    // Length is checked first because `ct_eq` on differing lengths is not a
    // meaningful comparison; the length of a signature is not a secret.
    if provided.len() == expected.len() && bool::from(provided.ct_eq(&expected)) {
        Ok(())
    } else {
        Err(TriggerError::BadSignature)
    }
}

/// Where a run's extracted tree and its archive live.
pub struct Workspace {
    pub root: PathBuf,
    pub archive: PathBuf,
}

impl Workspace {
    pub fn for_run(config: &Config, run_id: &str) -> Self {
        Self {
            root: config.workspace_dir.join(run_id),
            archive: config.workspace_dir.join(format!("{run_id}.tar.gz")),
        }
    }
}

/// Decode and extract a submitted archive into `workspace.root`, keeping the
/// compressed copy at `workspace.archive` for shipping to the guest.
///
/// Keeping the original rather than re-taring is not just an optimisation: a
/// round trip through extract-and-repack would silently normalise permissions
/// and drop anything the extractor chose not to write, so what runs in the guest
/// would differ from what was submitted.
pub fn materialize(
    source: &SourceArchive,
    workspace: &Workspace,
    max_bytes: usize,
) -> Result<usize, TriggerError> {
    if source.format != "tar.gz" {
        return Err(TriggerError::UnsupportedFormat(source.format.clone()));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(source.content_base64.as_bytes())
        .map_err(|e| TriggerError::BadArchive(format!("not valid base64: {e}")))?;
    if bytes.len() > max_bytes {
        return Err(TriggerError::ArchiveTooLarge {
            bytes: bytes.len(),
            max: max_bytes,
        });
    }

    if workspace.root.exists() {
        std::fs::remove_dir_all(&workspace.root).map_err(|e| TriggerError::Io {
            path: workspace.root.clone(),
            reason: e.to_string(),
        })?;
    }
    std::fs::create_dir_all(&workspace.root).map_err(|e| TriggerError::Io {
        path: workspace.root.clone(),
        reason: e.to_string(),
    })?;

    extract(&bytes, &workspace.root)?;

    std::fs::write(&workspace.archive, &bytes).map_err(|e| TriggerError::Io {
        path: workspace.archive.clone(),
        reason: e.to_string(),
    })?;
    Ok(bytes.len())
}

/// Extract a gzipped tar, refusing any entry that would write outside `root`.
///
/// The archive is unauthenticated input right up until the signature check
/// passes, and even then it is only as trustworthy as whoever holds the webhook
/// secret. `tar`'s own protections have historically varied by version, so every
/// path is checked here: no absolute paths, no `..`, and no symlink or hard link
/// pointing outside the tree.
fn extract(gz: &[u8], root: &Path) -> Result<(), TriggerError> {
    let decoder = flate2::read::GzDecoder::new(gz);
    let mut archive = tar::Archive::new(decoder);
    // Ownership from the archive is meaningless here and would need privileges.
    archive.set_preserve_permissions(true);
    archive.set_unpack_xattrs(false);

    let entries = archive
        .entries()
        .map_err(|e| TriggerError::BadArchive(e.to_string()))?;

    for entry in entries {
        let mut entry = entry.map_err(|e| TriggerError::BadArchive(e.to_string()))?;
        let path = entry
            .path()
            .map_err(|e| TriggerError::BadArchive(e.to_string()))?
            .into_owned();
        check_contained(&path)?;

        // A link's *target* escapes just as effectively as a path does.
        if let Ok(Some(link)) = entry.link_name() {
            check_contained(&link)?;
        }

        entry
            .unpack_in(root)
            .map_err(|e| TriggerError::BadArchive(format!("unpacking {}: {e}", path.display())))?;
    }
    Ok(())
}

/// Whether a path stays inside the extraction root.
fn check_contained(path: &Path) -> Result<(), TriggerError> {
    for c in path.components() {
        match c {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir => {
                return Err(TriggerError::EscapingEntry(path.display().to_string()));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(TriggerError::EscapingEntry(path.display().to_string()));
            }
        }
    }
    Ok(())
}

/// Find workflow files in an extracted tree.
///
/// `pattern` is the workflow object's `path`, e.g. `.ci/workflows/*.yml`. Only a
/// trailing `*` in the final segment is supported — enough for every real
/// spelling, and a full glob engine here would be a way to walk the filesystem
/// with a pattern that came over the wire.
pub fn find_workflows(root: &Path, pattern: &str) -> Result<Vec<(String, String)>, TriggerError> {
    let pattern = pattern.trim().trim_start_matches("./");
    check_contained(Path::new(pattern))?;

    let (dir_part, file_pattern) = match pattern.rsplit_once('/') {
        Some((d, f)) => (d, f),
        None => ("", pattern),
    };
    let dir = root.join(dir_part);

    let mut found = Vec::new();
    let Ok(read) = std::fs::read_dir(&dir) else {
        // A repository with no workflow directory is not an error; it just has
        // no workflows, and saying so beats a stat failure.
        return Ok(found);
    };
    for entry in read.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !matches_pattern(&name, file_pattern) {
            continue;
        }
        if !entry.path().is_file() {
            continue;
        }
        let text = std::fs::read_to_string(entry.path()).map_err(|e| TriggerError::Io {
            path: entry.path(),
            reason: e.to_string(),
        })?;
        let rel = if dir_part.is_empty() {
            name.clone()
        } else {
            format!("{dir_part}/{name}")
        };
        found.push((rel, text));
    }
    // Sorted so a run's workflows are always planned in the same order.
    found.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(found)
}

/// `*.yml`, `build.*`, `*`, or a literal name.
fn matches_pattern(name: &str, pattern: &str) -> bool {
    match pattern.split_once('*') {
        None => name == pattern,
        Some((prefix, suffix)) => {
            name.len() >= prefix.len() + suffix.len()
                && name.starts_with(prefix)
                && name.ends_with(suffix)
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TriggerError {
    MissingSignature,
    MalformedSignature,
    BadSignature,
    UnsupportedFormat(String),
    BadArchive(String),
    EscapingEntry(String),
    ArchiveTooLarge { bytes: usize, max: usize },
    Io { path: PathBuf, reason: String },
    NoWorkflows(String),
}

impl TriggerError {
    /// The HTTP status this should be reported as.
    pub fn status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::MissingSignature | Self::MalformedSignature | Self::BadSignature => {
                StatusCode::UNAUTHORIZED
            }
            Self::ArchiveTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Io { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

impl fmt::Display for TriggerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Deliberately identical wording for all three: telling a caller
            // *why* their signature failed tells an attacker how far they got.
            Self::MissingSignature | Self::MalformedSignature | Self::BadSignature => {
                write!(
                    f,
                    "the request carries no usable credential. Register the repository \
                     on /repos and set `git config ci.token`, or sign with the shared \
                     CI_WEBHOOK_SECRET and check that the client and the server agree \
                     on it."
                )
            }
            Self::UnsupportedFormat(fmt) => write!(
                f,
                "source archive format {fmt:?} is not supported; this server \
                 understands `tar.gz`. Upgrade `git submit`."
            ),
            Self::BadArchive(e) => write!(f, "the source archive could not be read: {e}"),
            Self::EscapingEntry(p) => write!(
                f,
                "the source archive contains {p:?}, which would write outside the \
                 workspace. Refusing the whole archive."
            ),
            Self::ArchiveTooLarge { bytes, max } => write!(
                f,
                "the source archive is {bytes} bytes, over the {max}-byte limit. \
                 Raise CI_MAX_SOURCE_BYTES, or exclude build output with a \
                 .gitattributes `export-ignore`."
            ),
            Self::Io { path, reason } => write!(f, "{}: {reason}", path.display()),
            Self::NoWorkflows(pattern) => write!(
                f,
                "no workflow files matched {pattern:?} in the submitted tree"
            ),
        }
    }
}

impl std::error::Error for TriggerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const SECRET: &str = "0123456789abcdef";

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn a_correct_signature_is_accepted() {
        let body = br#"{"hello":"world"}"#;
        let sig = sign(SECRET, body);
        assert_eq!(verify_signature(SECRET, body, Some(&sig)), Ok(()));
    }

    #[test]
    fn a_tampered_body_is_rejected() {
        let sig = sign(SECRET, b"original");
        assert_eq!(
            verify_signature(SECRET, b"tampered", Some(&sig)),
            Err(TriggerError::BadSignature)
        );
    }

    #[test]
    fn the_wrong_secret_is_rejected() {
        let body = b"body";
        let sig = sign("a-different-secret", body);
        assert_eq!(
            verify_signature(SECRET, body, Some(&sig)),
            Err(TriggerError::BadSignature)
        );
    }

    #[test]
    fn a_missing_or_malformed_signature_is_rejected() {
        assert_eq!(
            verify_signature(SECRET, b"x", None),
            Err(TriggerError::MissingSignature)
        );
        for bad in ["", "deadbeef", "sha256=zzz", "md5=abcd", "sha256="] {
            assert!(
                verify_signature(SECRET, b"x", Some(bad)).is_err(),
                "{bad:?} must be refused"
            );
        }
    }

    /// All three failures must read identically. Distinguishing them tells an
    /// attacker whether they got the format right, which is a rung on the ladder.
    #[test]
    fn every_signature_failure_reads_the_same() {
        let a = TriggerError::MissingSignature.to_string();
        let b = TriggerError::MalformedSignature.to_string();
        let c = TriggerError::BadSignature.to_string();
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(TriggerError::BadSignature.status(), 401);
    }

    fn tarball(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut ar = tar::Builder::new(Vec::new());
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            ar.append_data(&mut header, name, *content).unwrap();
        }
        let tar = ar.into_inner().unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(&tar).unwrap();
        gz.finish().unwrap()
    }

    fn source(bytes: &[u8]) -> SourceArchive {
        SourceArchive {
            format: "tar.gz".into(),
            content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        }
    }

    fn workspace() -> Workspace {
        let base = std::env::temp_dir().join(format!("ci-trigger-{}", crate::vm::new_id()));
        std::fs::create_dir_all(&base).unwrap();
        Workspace {
            root: base.join("tree"),
            archive: base.join("source.tar.gz"),
        }
    }

    #[test]
    fn a_tree_extracts_and_the_archive_is_kept() {
        let ws = workspace();
        let gz = tarball(&[
            ("Cargo.lock", b"version = 3"),
            (".ci/workflows/build.yml", b"name: build"),
        ]);
        let n = materialize(&source(&gz), &ws, 1 << 20).unwrap();
        assert_eq!(n, gz.len());
        assert_eq!(
            std::fs::read_to_string(ws.root.join("Cargo.lock")).unwrap(),
            "version = 3"
        );
        assert!(
            ws.archive.exists(),
            "the original archive is kept for the guest"
        );
        assert_eq!(std::fs::read(&ws.archive).unwrap(), gz);
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    /// Build a tar entry whose name (and optionally link target) bypasses the
    /// `tar` crate's own write-side validation.
    ///
    /// `Builder::append_data` refuses a path containing `..`, so a hostile
    /// archive cannot be produced through the safe API — which is exactly why
    /// the header bytes are written directly here. A real attacker has no such
    /// constraint: they emit the bytes.
    fn raw_entry(name: &str, link: Option<&str>, content: &[u8]) -> Vec<u8> {
        let mut header = tar::Header::new_gnu();
        header.set_size(if link.is_some() {
            0
        } else {
            content.len() as u64
        });
        header.set_mode(0o644);
        header.set_entry_type(if link.is_some() {
            tar::EntryType::Symlink
        } else {
            tar::EntryType::Regular
        });

        // Write the name straight into the 100-byte field, past validation.
        {
            let old = header.as_old_mut();
            let bytes = name.as_bytes();
            old.name[..bytes.len()].copy_from_slice(bytes);
            if let Some(link) = link {
                let lb = link.as_bytes();
                old.linkname[..lb.len()].copy_from_slice(lb);
            }
        }
        header.set_cksum();

        let mut out = header.as_bytes().to_vec();
        if link.is_none() {
            out.extend_from_slice(content);
            // Entries are padded to a 512-byte boundary.
            let pad = (512 - (content.len() % 512)) % 512;
            out.extend(std::iter::repeat_n(0u8, pad));
        }
        out
    }

    fn gzip(tar_bytes: Vec<u8>) -> Vec<u8> {
        let mut full = tar_bytes;
        // Two zero blocks end an archive.
        full.extend(std::iter::repeat_n(0u8, 1024));
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(&full).unwrap();
        gz.finish().unwrap()
    }

    /// The headline security property: an archive is attacker-supplied, and one
    /// `..` entry would let a submit overwrite anything the process can write.
    #[test]
    fn an_entry_escaping_the_workspace_rejects_the_whole_archive() {
        for evil in ["../escaped.txt", "a/../../escaped.txt", "/etc/passwd"] {
            let ws = workspace();
            let mut bytes = raw_entry("safe.txt", None, b"ok");
            bytes.extend(raw_entry(evil, None, b"pwned"));
            let gz = gzip(bytes);

            let err = materialize(&source(&gz), &ws, 1 << 20).unwrap_err();
            assert!(
                matches!(err, TriggerError::EscapingEntry(_)),
                "{evil:?} produced {err:?}"
            );
            let outside = ws.root.parent().unwrap().join("escaped.txt");
            assert!(!outside.exists(), "{evil:?} wrote outside the workspace");
            assert!(
                !Path::new("/etc/passwd.ci-test").exists(),
                "absolute paths must not be honoured"
            );
            std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
        }
    }

    /// A symlink escapes just as well as a path does: extract `link ->
    /// ../../etc/passwd` and a later entry writing through `link` lands outside.
    #[test]
    fn a_symlink_pointing_outside_is_rejected() {
        let ws = workspace();
        let gz = gzip(raw_entry("link", Some("../../../etc/passwd"), b""));
        let err = materialize(&source(&gz), &ws, 1 << 20).unwrap_err();
        assert!(matches!(err, TriggerError::EscapingEntry(_)), "{err:?}");
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    /// A symlink that stays inside the tree is ordinary and must survive —
    /// plenty of repositories contain one.
    #[test]
    fn a_symlink_inside_the_tree_is_allowed() {
        let ws = workspace();
        let mut bytes = raw_entry("real.txt", None, b"hi");
        bytes.extend(raw_entry("alias.txt", Some("real.txt"), b""));
        let gz = gzip(bytes);
        materialize(&source(&gz), &ws, 1 << 20).expect("an internal symlink is fine");
        assert!(ws.root.join("alias.txt").symlink_metadata().is_ok());
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    #[test]
    fn an_oversized_archive_is_refused_with_the_limit_named() {
        let ws = workspace();
        let gz = tarball(&[("big.bin", &vec![0u8; 4096])]);
        let err = materialize(&source(&gz), &ws, 8).unwrap_err();
        match &err {
            TriggerError::ArchiveTooLarge { max, .. } => assert_eq!(*max, 8),
            other => panic!("{other:?}"),
        }
        assert!(err.to_string().contains("CI_MAX_SOURCE_BYTES"), "{err}");
        assert_eq!(err.status(), 413);
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    #[test]
    fn a_non_targz_format_is_refused_by_name() {
        let ws = workspace();
        let s = SourceArchive {
            format: "zip".into(),
            content_base64: String::new(),
        };
        let err = materialize(&s, &ws, 1 << 20).unwrap_err();
        assert!(matches!(err, TriggerError::UnsupportedFormat(_)), "{err:?}");
        assert!(err.to_string().contains("tar.gz"), "{err}");
    }

    /// Re-submitting the same run must not leave a previous tree's files behind,
    /// or a deleted file would still be there for the fingerprint to hash.
    #[test]
    fn materializing_twice_replaces_the_tree_rather_than_merging() {
        let ws = workspace();
        materialize(&source(&tarball(&[("old.txt", b"1")])), &ws, 1 << 20).unwrap();
        assert!(ws.root.join("old.txt").exists());
        materialize(&source(&tarball(&[("new.txt", b"2")])), &ws, 1 << 20).unwrap();
        assert!(ws.root.join("new.txt").exists());
        assert!(
            !ws.root.join("old.txt").exists(),
            "a file removed on the branch must be gone from the workspace"
        );
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    #[test]
    fn workflows_are_found_by_pattern_and_returned_sorted() {
        let ws = workspace();
        let gz = tarball(&[
            (".ci/workflows/b.yml", b"name: b"),
            (".ci/workflows/a.yml", b"name: a"),
            (".ci/workflows/notes.md", b"not a workflow"),
            ("src/main.rs", b"fn main() {}"),
        ]);
        materialize(&source(&gz), &ws, 1 << 20).unwrap();

        let found = find_workflows(&ws.root, ".ci/workflows/*.yml").unwrap();
        let names: Vec<&str> = found.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(names, [".ci/workflows/a.yml", ".ci/workflows/b.yml"]);
        assert_eq!(found[0].1, "name: a");
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    /// A repository with no workflow directory has no workflows; that is not a
    /// failure to stat something.
    #[test]
    fn a_tree_with_no_workflow_directory_yields_nothing() {
        let ws = workspace();
        materialize(&source(&tarball(&[("README", b"hi")])), &ws, 1 << 20).unwrap();
        assert!(
            find_workflows(&ws.root, ".ci/workflows/*.yml")
                .unwrap()
                .is_empty()
        );
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    /// The pattern arrives from a workflow object, which an operator wrote — but
    /// it still must not be able to read outside the submitted tree.
    #[test]
    fn a_workflow_pattern_cannot_escape_the_tree() {
        let ws = workspace();
        materialize(&source(&tarball(&[("README", b"hi")])), &ws, 1 << 20).unwrap();
        assert!(matches!(
            find_workflows(&ws.root, "../../etc/*"),
            Err(TriggerError::EscapingEntry(_))
        ));
        std::fs::remove_dir_all(ws.root.parent().unwrap()).ok();
    }

    #[test]
    fn patterns_match_the_shapes_people_write() {
        assert!(matches_pattern("build.yml", "*.yml"));
        assert!(matches_pattern("a.yaml", "*"));
        assert!(matches_pattern("build.yml", "build.*"));
        assert!(matches_pattern("build.yml", "build.yml"));
        assert!(!matches_pattern("build.yaml", "*.yml"));
        assert!(!matches_pattern("notes.md", "*.yml"));
        // A pattern must not match a shorter name by overlapping its own
        // prefix and suffix: `*.yml` must not match `.yml`'s own dot.
        assert!(!matches_pattern(".yml", "*x.yml"));
    }

    #[test]
    fn a_ref_reduces_to_its_branch() {
        let mut r = SubmitRequest {
            repository: RepositoryRef::default(),
            r#ref: "refs/heads/feature/x".into(),
            before: String::new(),
            after: String::new(),
            dry_run: false,
            pusher: None,
            workflow_id: None,
            source: SourceArchive {
                format: "tar.gz".into(),
                content_base64: String::new(),
            },
        };
        assert_eq!(r.branch(), "feature/x");
        r.r#ref = "main".into();
        assert_eq!(r.branch(), "main");
    }
}
