//! The workflow file: `.ci/workflows/*.yml`.
//!
//! GitHub Actions' shape, with two deliberate departures.
//!
//! **`uses:` on a job places it**, where GitHub has `runs-on:` selecting a
//! label. That is the point of the whole system: a job names the heyvm network
//! and the machine it wants, and membership of that network is what makes a host
//! eligible. The grammar is `default`, `<network>`, `<network>/<node>` or
//! `<network>/<node>/<vm>` — see [`Target`].
//!
//! **`vm:` on a job describes the machine to build.** GitHub gives you an opaque
//! runner image; here the workflow author declares the driver, image, size and
//! setup hooks, and — via `cache_key_files` — says what should invalidate the
//! warm VM the next run would otherwise reuse.
//!
//! Parsing is two-stage on purpose. The document is read as a
//! `serde_yaml::Value` first so that `jobs:` keeps the order the author wrote
//! (a `BTreeMap` would sort them, and a workflow reads top to bottom) and so
//! that a malformed job reports *which* job rather than a line number inside a
//! merged error.

use crate::vm::VmSpec;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The literal `uses:` value meaning "the host this orchestrator runs on".
pub const DEFAULT_NODE: &str = "default";

/// Where a job runs, as `uses:` spells it.
///
/// **`uses:` carries everything needed to place the job**, which is what makes
/// the third form worth having: a sandbox does not record which host it is on
/// (`SandboxInfo` has no daemon field, and there is no cloud-proxied exec), so
/// without the node in the path the orchestrator would have to interrogate every
/// host in the network to find one VM.
///
/// ```text
/// default                     the host this orchestrator runs on
/// <network>                   any online host in that network
/// <network>/*                 the same, written explicitly
/// <network>/<node>            that host; the `vm:` block builds a VM on it
/// <network>/<node>/<vm>       that existing VM on that host; `vm:` is unused
///                             and every step is an exec into it
/// absent                      the repository's assigned network, any host
/// ```
///
/// Kept as a struct with defaulted fields rather than an enum, because a
/// `JobPlan` — this included — is persisted as JSONB on `ci_job` and replayed on
/// a JetStream redelivery. An enum would change that shape and strand every job
/// queued across the deploy that introduced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    /// `None` means the repository's assigned network — or, with `local`, no
    /// network at all.
    pub network: Option<String>,
    /// The host machine, by daemon id or name. `None` means any online host in
    /// the network.
    ///
    /// Aliased to `runner` so a plan written by an older build still
    /// deserializes: that is the field name jobs queued before this change
    /// carry, and a redelivery has to route them the same way.
    #[serde(alias = "runner")]
    pub node: Option<String>,
    /// An existing sandbox on that node. When set, the `vm:` block is unused and
    /// every step execs into this VM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm: Option<String>,
    /// `uses: default` — resolve to the orchestrator's own host, whatever
    /// network it is in.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub local: bool,
}

impl Target {
    /// Parse the `uses:` value.
    pub fn parse(raw: &str) -> Result<Self, WorkflowError> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(WorkflowError::EmptyUses);
        }
        if raw.eq_ignore_ascii_case(DEFAULT_NODE) {
            return Ok(Self {
                network: None,
                node: None,
                vm: None,
                local: true,
            });
        }

        let parts: Vec<&str> = raw.split('/').map(str::trim).collect();
        if parts.len() > 3 {
            return Err(WorkflowError::UsesTooDeep(raw.to_string()));
        }
        let network = parts[0];
        if network.is_empty() {
            return Err(WorkflowError::EmptyUses);
        }

        // `*` means "unspecified" only in the node position. A `*` network would
        // be "any network at all", which is not a placement anybody means, and a
        // `*` VM cannot be exec'd into.
        let node = match parts.get(1).copied() {
            None | Some("*") => None,
            Some("") => return Err(WorkflowError::EmptyRunner(raw.to_string())),
            Some(n) => Some(n.to_string()),
        };
        let vm = match parts.get(2).copied() {
            None => None,
            Some("") | Some("*") => return Err(WorkflowError::EmptyRunner(raw.to_string())),
            Some(v) => Some(v.to_string()),
        };
        // `<network>/*/<vm>` names a VM without saying which host holds it,
        // which is precisely the lookup this grammar exists to avoid.
        if vm.is_some() && node.is_none() {
            return Err(WorkflowError::VmWithoutNode(raw.to_string()));
        }

        Ok(Self {
            network: Some(network.to_string()),
            node,
            vm,
            local: false,
        })
    }

    pub fn any() -> Self {
        Self {
            network: None,
            node: None,
            vm: None,
            local: false,
        }
    }

    /// Whether steps exec into an existing VM rather than one built from the
    /// `vm:` block.
    pub fn is_existing_vm(&self) -> bool {
        self.vm.is_some()
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.local {
            return write!(f, "{DEFAULT_NODE}");
        }
        let network = self.network.as_deref().unwrap_or("*");
        match (&self.node, &self.vm) {
            (Some(n), Some(v)) => write!(f, "{network}/{n}/{v}"),
            (Some(n), None) => write!(f, "{network}/{n}"),
            (None, _) => write!(f, "{network}/*"),
        }
    }
}

/// What to do when the named runner cannot take the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Fallback {
    /// Wait for that host, then fail. The default, because a warm VM pool is
    /// host-local: silently moving the job to another host throws away the cache
    /// the author asked for by pinning it, and turns a fast build into a slow
    /// one for reasons nothing reports.
    #[default]
    None,
    /// Any online host in the network will do.
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "if", default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uses: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub with: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
    #[serde(
        rename = "working-directory",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub working_directory: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(
        rename = "timeout-minutes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_minutes: Option<u64>,
    #[serde(rename = "continue-on-error", default)]
    pub continue_on_error: bool,
}

impl Step {
    pub fn label(&self, index: usize) -> String {
        if let Some(n) = &self.name {
            return n.clone();
        }
        if let Some(u) = &self.uses {
            return u.clone();
        }
        if let Some(r) = &self.run {
            // First line only — a `run:` block is often a whole script, and a
            // step list is unreadable if one entry is forty lines.
            let first = r
                .lines()
                .find(|l| !l.trim().is_empty())
                .unwrap_or("")
                .trim();
            return truncate(first, 60);
        }
        format!("step {}", index + 1)
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let cut: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{cut}…")
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Strategy {
    /// Axes, or an explicit `include:` list. Values are free-form YAML so a
    /// numeric axis stays numeric for `${{ }}` comparisons.
    #[serde(default)]
    pub matrix: Option<serde_yaml::Value>,
    #[serde(rename = "max-parallel", default)]
    pub max_parallel: Option<usize>,
    #[serde(rename = "fail-fast", default = "default_true")]
    pub fail_fast: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `<network>/<runner>`. Absent means the workflow object's network and any
    /// online host in it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uses: Option<String>,
    pub vm: VmSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<String>,
    #[serde(rename = "if", default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<Strategy>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outputs: BTreeMap<String, String>,
    #[serde(rename = "continue-on-error", default)]
    pub continue_on_error: bool,
    #[serde(
        rename = "timeout-minutes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_minutes: Option<u64>,
    #[serde(default)]
    pub fallback: Fallback,
    pub steps: Vec<Step>,
}

impl Job {
    pub fn target(&self) -> Result<Target, WorkflowError> {
        match &self.uses {
            Some(u) => Target::parse(u),
            None => Ok(Target::any()),
        }
    }
}

/// A parsed workflow file. `jobs` keeps the author's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workflow {
    pub path: String,
    pub name: Option<String>,
    /// Trigger names from `on:`. Only `submit` is honoured today; anything else
    /// parses and is reported as unsupported rather than silently ignored.
    pub on: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub jobs: Vec<(String, Job)>,
}

/// The top level, minus `jobs`, which is handled separately to keep its order.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowHeader {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    on: Option<serde_yaml::Value>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    jobs: serde_yaml::Value,
}

impl Workflow {
    pub fn parse(path: &str, yaml: &str) -> Result<Self, WorkflowError> {
        let header: WorkflowHeader =
            serde_yaml::from_str(yaml).map_err(|e| WorkflowError::Yaml {
                path: path.to_string(),
                detail: e.to_string(),
            })?;

        let Some(mapping) = header.jobs.as_mapping() else {
            return Err(WorkflowError::NoJobs(path.to_string()));
        };
        if mapping.is_empty() {
            return Err(WorkflowError::NoJobs(path.to_string()));
        }

        let mut jobs = Vec::with_capacity(mapping.len());
        for (key, value) in mapping {
            let id = key
                .as_str()
                .ok_or_else(|| WorkflowError::BadJobId(format!("{key:?}")))?
                .to_string();
            // Deserialized one at a time so the error names the job. A single
            // `BTreeMap<String, Job>` deserialize reports a path into the merged
            // document, which is far harder to act on.
            let job: Job =
                serde_yaml::from_value(value.clone()).map_err(|e| WorkflowError::BadJob {
                    path: path.to_string(),
                    job: id.clone(),
                    detail: e.to_string(),
                })?;
            jobs.push((id, job));
        }

        let on = match header.on {
            None => vec!["submit".to_string()],
            Some(v) => trigger_names(&v),
        };

        let wf = Self {
            path: path.to_string(),
            name: header.name,
            on,
            env: header.env,
            jobs,
        };
        wf.validate()?;
        Ok(wf)
    }

    fn validate(&self) -> Result<(), WorkflowError> {
        let ids: BTreeSet<&str> = self.jobs.iter().map(|(id, _)| id.as_str()).collect();
        if ids.len() != self.jobs.len() {
            return Err(WorkflowError::DuplicateJob(self.path.clone()));
        }

        for (id, job) in &self.jobs {
            if !is_job_id(id) {
                return Err(WorkflowError::BadJobId(id.clone()));
            }
            job.vm.validate().map_err(|e| WorkflowError::BadVm {
                job: id.clone(),
                detail: e.to_string(),
            })?;
            job.target().map_err(|e| match e {
                WorkflowError::EmptyUses => WorkflowError::BadUses {
                    job: id.clone(),
                    detail: "`uses:` is empty".to_string(),
                },
                other => WorkflowError::BadUses {
                    job: id.clone(),
                    detail: other.to_string(),
                },
            })?;

            for need in &job.needs {
                if !ids.contains(need.as_str()) {
                    return Err(WorkflowError::UnknownNeed {
                        job: id.clone(),
                        need: need.clone(),
                    });
                }
                if need == id {
                    return Err(WorkflowError::SelfNeed(id.clone()));
                }
            }

            if job.steps.is_empty() {
                return Err(WorkflowError::NoSteps(id.clone()));
            }
            for (i, step) in job.steps.iter().enumerate() {
                match (&step.run, &step.uses) {
                    (Some(_), Some(_)) => {
                        return Err(WorkflowError::StepRunAndUses {
                            job: id.clone(),
                            step: step.label(i),
                        });
                    }
                    (None, None) => {
                        return Err(WorkflowError::StepNeitherRunNorUses {
                            job: id.clone(),
                            step: step.label(i),
                        });
                    }
                    _ => {}
                }
            }
        }

        // A cycle would otherwise surface as jobs that simply never start, with
        // nothing saying why.
        if let Some(cycle) = self.find_cycle() {
            return Err(WorkflowError::Cycle(cycle));
        }
        Ok(())
    }

    /// Depth-first search for a `needs` cycle, returning the path if there is one.
    fn find_cycle(&self) -> Option<Vec<String>> {
        #[derive(Clone, Copy, PartialEq)]
        enum Mark {
            Open,
            Done,
        }
        let deps: BTreeMap<&str, &Vec<String>> = self
            .jobs
            .iter()
            .map(|(id, j)| (id.as_str(), &j.needs))
            .collect();
        let mut marks: BTreeMap<&str, Mark> = BTreeMap::new();
        let mut stack: Vec<&str> = Vec::new();

        fn visit<'a>(
            node: &'a str,
            deps: &BTreeMap<&'a str, &'a Vec<String>>,
            marks: &mut BTreeMap<&'a str, Mark>,
            stack: &mut Vec<&'a str>,
        ) -> Option<Vec<String>> {
            match marks.get(node) {
                Some(Mark::Done) => return None,
                Some(Mark::Open) => {
                    let start = stack.iter().position(|n| *n == node).unwrap_or(0);
                    let mut cycle: Vec<String> =
                        stack[start..].iter().map(|s| s.to_string()).collect();
                    cycle.push(node.to_string());
                    return Some(cycle);
                }
                None => {}
            }
            marks.insert(node, Mark::Open);
            stack.push(node);
            if let Some(needs) = deps.get(node) {
                for n in needs.iter() {
                    if let Some((key, _)) = deps.get_key_value(n.as_str())
                        && let Some(c) = visit(key, deps, marks, stack)
                    {
                        return Some(c);
                    }
                }
            }
            stack.pop();
            marks.insert(node, Mark::Done);
            None
        }

        for (id, _) in &self.jobs {
            if let Some((key, _)) = deps.get_key_value(id.as_str())
                && let Some(c) = visit(key, &deps, &mut marks, &mut stack)
            {
                return Some(c);
            }
        }
        None
    }

    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.path)
    }
}

/// `on: submit`, `on: [submit, schedule]`, or `on: {submit: {...}}`.
fn trigger_names(v: &serde_yaml::Value) -> Vec<String> {
    match v {
        serde_yaml::Value::String(s) => vec![s.clone()],
        serde_yaml::Value::Sequence(items) => items
            .iter()
            .filter_map(|i| i.as_str().map(str::to_string))
            .collect(),
        serde_yaml::Value::Mapping(m) => m
            .keys()
            .filter_map(|k| k.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

/// Job ids become NATS subject tokens and appear in sandbox names, so the
/// alphabet is restricted rather than escaped — the same rule queue-fn applies
/// to function ids.
pub fn is_job_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkflowError {
    Yaml {
        path: String,
        detail: String,
    },
    NoJobs(String),
    DuplicateJob(String),
    BadJobId(String),
    BadJob {
        path: String,
        job: String,
        detail: String,
    },
    BadVm {
        job: String,
        detail: String,
    },
    BadUses {
        job: String,
        detail: String,
    },
    EmptyUses,
    EmptyRunner(String),
    /// `uses:` had more than the three segments the grammar defines.
    UsesTooDeep(String),
    /// A VM named without the host that holds it.
    VmWithoutNode(String),
    UnknownNeed {
        job: String,
        need: String,
    },
    SelfNeed(String),
    Cycle(Vec<String>),
    NoSteps(String),
    StepRunAndUses {
        job: String,
        step: String,
    },
    StepNeitherRunNorUses {
        job: String,
        step: String,
    },
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Yaml { path, detail } => write!(f, "{path} is not valid workflow YAML: {detail}"),
            Self::NoJobs(path) => write!(f, "{path} declares no jobs"),
            Self::DuplicateJob(path) => write!(f, "{path} declares the same job id twice"),
            Self::BadJobId(id) => write!(
                f,
                "job id {id:?} must be 1-64 characters of letters, digits, '_' or '-'; \
                 it becomes a NATS subject token and part of a VM name"
            ),
            Self::BadJob { path, job, detail } => {
                write!(f, "job {job:?} in {path} is malformed: {detail}")
            }
            Self::BadVm { job, detail } => write!(f, "job {job:?} has an invalid `vm:` — {detail}"),
            Self::BadUses { job, detail } => {
                write!(f, "job {job:?} has an invalid `uses:` — {detail}")
            }
            Self::EmptyUses => write!(
                f,
                "`uses:` must name a network, as `<network>` or `<network>/<runner>`"
            ),
            Self::EmptyRunner(raw) => write!(
                f,
                "`uses: {raw}` has an empty segment; write `<network>`, \
                 `<network>/<node>`, or `<network>/<node>/<vm>`"
            ),
            Self::UsesTooDeep(raw) => write!(
                f,
                "`uses: {raw}` has more than three segments. The deepest form is \
                 `<network>/<node>/<vm>`, which names an existing VM on a host; \
                 `default` names the host this CI runs on."
            ),
            Self::VmWithoutNode(raw) => write!(
                f,
                "`uses: {raw}` names a VM but not the host holding it. A sandbox \
                 does not record its host, so write `<network>/<node>/<vm>` — \
                 otherwise every host in the network has to be interrogated to \
                 find it."
            ),
            Self::UnknownNeed { job, need } => write!(
                f,
                "job {job:?} needs {need:?}, which no job in this workflow declares"
            ),
            Self::SelfNeed(job) => write!(f, "job {job:?} needs itself"),
            Self::Cycle(path) => write!(f, "jobs form a `needs` cycle: {}", path.join(" -> ")),
            Self::NoSteps(job) => write!(f, "job {job:?} has no steps"),
            Self::StepRunAndUses { job, step } => write!(
                f,
                "step {step:?} in job {job:?} sets both `run:` and `uses:`; a step is one \
                 or the other"
            ),
            Self::StepNeitherRunNorUses { job, step } => write!(
                f,
                "step {step:?} in job {job:?} sets neither `run:` nor `uses:`"
            ),
        }
    }
}

impl std::error::Error for WorkflowError {}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
name: build
on: [submit]
env:
  CARGO_TERM_COLOR: never

jobs:
  build:
    uses: prod-runners/bigbox
    vm:
      driver: firecracker
      image: ubuntu:24.04
      size_class: medium
      cache_key_files:
        - Cargo.lock
    strategy:
      matrix:
        target: [x86_64, aarch64]
      max-parallel: 2
    steps:
      - name: Build
        run: cargo build --release --target ${{ matrix.target }}
        timeout-minutes: 30
      - uses: ci/upload-artifact
        with:
          name: bin
          path: target/release/app

  deploy:
    uses: prod-runners
    needs: [build]
    if: ${{ needs.build.result == 'success' }}
    vm:
      driver: kvm
      image: ubuntu:24.04
    steps:
      - run: ./deploy.sh
"#;

    fn parse(yaml: &str) -> Result<Workflow, WorkflowError> {
        Workflow::parse(".ci/workflows/build.yml", yaml)
    }

    /// This repository's own `.ci/workflows/*.yml`, parsed and planned from
    /// disk.
    ///
    /// Everywhere else a workflow is a string in a test. These files are the
    /// real thing: they are submitted to a live orchestrator, where a typo is a
    /// rejected submit somebody is waiting on rather than a compile error — and
    /// `deny_unknown_fields` means a misspelling like `stpes:` is a hard parse
    /// failure. This repository also builds itself, so its own workflow being
    /// wrong breaks its own CI. Reading them here moves that failure into
    /// `cargo test`, where it costs seconds.
    #[test]
    fn this_repositorys_own_workflows_parse_and_plan() {
        let dir = std::path::Path::new(".ci/workflows");
        let mut checked = 0;
        for entry in std::fs::read_dir(dir)
            .expect(".ci/workflows exists")
            .flatten()
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("yml") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("readable");
            let wf = Workflow::parse(&path.display().to_string(), &text)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            assert!(
                wf.on.iter().any(|t| t == "submit"),
                "{} does not trigger on `submit`, so a submit would silently \
                 skip it and report that nothing matched",
                path.display()
            );
            crate::plan::Plan::build(&wf).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            checked += 1;
        }
        // Without this the test passes by finding nothing, which is exactly what
        // happens if the directory is ever renamed.
        assert!(checked > 0, "no workflow files found in {}", dir.display());
    }

    #[test]
    fn the_sample_workflow_parses() {
        let wf = parse(SAMPLE).expect("parses");
        assert_eq!(wf.name.as_deref(), Some("build"));
        assert_eq!(wf.on, ["submit"]);
        assert_eq!(wf.env.get("CARGO_TERM_COLOR").unwrap(), "never");
        assert_eq!(wf.jobs.len(), 2);
    }

    /// A workflow reads top to bottom; a `BTreeMap` would render `deploy`
    /// before `build`.
    #[test]
    fn jobs_keep_the_order_the_author_wrote() {
        let wf = parse(SAMPLE).unwrap();
        let ids: Vec<&str> = wf.jobs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["build", "deploy"]);
    }

    #[test]
    fn uses_names_a_network_a_node_and_optionally_a_vm() {
        assert_eq!(
            Target::parse("prod-runners/bigbox").unwrap(),
            Target {
                network: Some("prod-runners".into()),
                node: Some("bigbox".into()),
                vm: None,
                local: false,
            }
        );
        // Three segments: an existing VM on a named host. The `vm:` block is
        // unused and every step execs into it.
        let existing = Target::parse("prod-runners/bigbox/sb-1a34").unwrap();
        assert_eq!(
            existing,
            Target {
                network: Some("prod-runners".into()),
                node: Some("bigbox".into()),
                vm: Some("sb-1a34".into()),
                local: false,
            }
        );
        assert!(existing.is_existing_vm());
        assert!(
            !Target::parse("prod-runners/bigbox")
                .unwrap()
                .is_existing_vm()
        );

        // Both spellings of "any host in this network".
        for raw in ["prod-runners", "prod-runners/*"] {
            assert_eq!(
                Target::parse(raw).unwrap(),
                Target {
                    network: Some("prod-runners".into()),
                    node: None,
                    vm: None,
                    local: false,
                },
                "{raw}"
            );
        }

        assert_eq!(Target::parse("  ").unwrap_err(), WorkflowError::EmptyUses);
        assert!(matches!(
            Target::parse("net/").unwrap_err(),
            WorkflowError::EmptyRunner(_)
        ));
    }

    /// `default` is the one form that names no network: it is whatever host
    /// this orchestrator is running on.
    #[test]
    fn default_names_the_orchestrators_own_host() {
        for raw in ["default", "DEFAULT", "  default  "] {
            let t = Target::parse(raw).unwrap_or_else(|e| panic!("{raw:?}: {e}"));
            assert!(t.local, "{raw}");
            assert_eq!(t.network, None);
            assert_eq!(t.node, None);
            assert_eq!(t.vm, None);
            assert_eq!(t.to_string(), "default");
        }
        // Absent `uses:` is a different thing: the repository's assigned
        // network, any host. Conflating the two would send every unpinned job
        // to the orchestrator's own machine.
        assert!(!Target::any().local);
    }

    /// The grammar exists so `uses:` alone can place a job. A VM without its
    /// host would put the lookup back, since a sandbox does not record one.
    #[test]
    fn a_vm_must_be_named_with_its_host() {
        assert!(matches!(
            Target::parse("prod-runners/*/sb-1").unwrap_err(),
            WorkflowError::VmWithoutNode(_)
        ));
        let e = Target::parse("prod-runners/*/sb-1")
            .unwrap_err()
            .to_string();
        assert!(e.contains("does not record its host"), "{e}");

        assert!(matches!(
            Target::parse("a/b/c/d").unwrap_err(),
            WorkflowError::UsesTooDeep(_)
        ));
        // A `*` in the VM position cannot be exec'd into, so it is refused
        // rather than quietly meaning "build one".
        assert!(matches!(
            Target::parse("net/node/*").unwrap_err(),
            WorkflowError::EmptyRunner(_)
        ));
    }

    /// Every form has to survive the round trip through `ci_job.plan`, and a
    /// plan written before `node` existed still has to route.
    #[test]
    fn targets_round_trip_through_the_stored_plan() {
        for raw in [
            "default",
            "prod-runners",
            "prod-runners/bigbox",
            "prod-runners/bigbox/sb-1a34",
        ] {
            let t = Target::parse(raw).unwrap();
            let json = serde_json::to_string(&t).unwrap();
            let back: Target = serde_json::from_str(&json).unwrap();
            assert_eq!(t, back, "{raw} -> {json}");
        }

        // The compatibility that matters on deploy day: a job queued by an
        // older build spells the host `runner`.
        let old: Target =
            serde_json::from_str(r#"{"network":"prod-runners","runner":"bigbox"}"#).unwrap();
        assert_eq!(old.node.as_deref(), Some("bigbox"));
        assert_eq!(old.vm, None);
        assert!(!old.local);
    }

    /// An absent `uses:` inherits the workflow object's network.
    #[test]
    fn a_job_without_uses_targets_anything() {
        let wf = parse(
            r#"
jobs:
  a:
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap();
        assert_eq!(wf.jobs[0].1.target().unwrap(), Target::any());
    }

    #[test]
    fn pinning_does_not_silently_migrate_by_default() {
        let wf = parse(SAMPLE).unwrap();
        assert_eq!(wf.jobs[0].1.fallback, Fallback::None);
        let wf = parse(
            r#"
jobs:
  a:
    uses: net/host
    fallback: any
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap();
        assert_eq!(wf.jobs[0].1.fallback, Fallback::Any);
    }

    #[test]
    fn a_needs_cycle_is_reported_with_its_path() {
        let err = parse(
            r#"
jobs:
  a:
    needs: [c]
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
  b:
    needs: [a]
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
  c:
    needs: [b]
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap_err();
        match &err {
            WorkflowError::Cycle(path) => {
                assert!(path.len() >= 3, "{path:?}");
                assert!(err.to_string().contains("->"), "{err}");
            }
            other => panic!("expected a cycle, got {other:?}"),
        }
    }

    #[test]
    fn a_job_needing_itself_is_rejected() {
        let err = parse(
            r#"
jobs:
  a:
    needs: [a]
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap_err();
        assert_eq!(err, WorkflowError::SelfNeed("a".into()));
    }

    #[test]
    fn a_need_on_an_unknown_job_names_both() {
        let err = parse(
            r#"
jobs:
  a:
    needs: [nope]
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            WorkflowError::UnknownNeed {
                job: "a".into(),
                need: "nope".into()
            }
        );
        assert!(err.to_string().contains("nope"));
    }

    /// The error has to name the job, not a line offset into a merged document.
    #[test]
    fn a_malformed_job_names_the_job() {
        let err = parse(
            r#"
jobs:
  build:
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
  broken:
    vm: { driver: nonsense }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap_err();
        match &err {
            WorkflowError::BadJob { job, .. } => assert_eq!(job, "broken"),
            other => panic!("expected BadJob, got {other:?}"),
        }
        assert!(err.to_string().contains("broken"), "{err}");
    }

    #[test]
    fn a_step_must_be_run_or_uses_but_not_both() {
        let err = parse(
            r#"
jobs:
  a:
    vm: { driver: firecracker }
    steps:
      - run: "true"
        uses: ci/upload-artifact
"#,
        )
        .unwrap_err();
        assert!(
            matches!(err, WorkflowError::StepRunAndUses { .. }),
            "{err:?}"
        );

        let err = parse(
            r#"
jobs:
  a:
    vm: { driver: firecracker }
    steps:
      - name: nothing
"#,
        )
        .unwrap_err();
        assert!(
            matches!(err, WorkflowError::StepNeitherRunNorUses { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn libvirt_is_rejected_and_names_the_job() {
        let err = parse(
            r#"
jobs:
  a:
    vm: { driver: libvirt }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap_err();
        match &err {
            WorkflowError::BadVm { job, detail } => {
                assert_eq!(job, "a");
                assert!(detail.contains("firecracker"), "{detail}");
            }
            other => panic!("expected BadVm, got {other:?}"),
        }
    }

    /// Job ids reach NATS subjects and VM names unescaped.
    #[test]
    fn job_ids_are_charset_restricted() {
        assert!(is_job_id("build"));
        assert!(is_job_id("build-x86_64"));
        assert!(!is_job_id(""));
        assert!(!is_job_id("build.release"));
        assert!(!is_job_id("build release"));
        assert!(!is_job_id(&"a".repeat(65)));

        let err = parse(
            r#"
jobs:
  "build.release":
    vm: { driver: firecracker }
    steps: [{ run: "true" }]
"#,
        )
        .unwrap_err();
        assert!(matches!(err, WorkflowError::BadJobId(_)), "{err:?}");
    }

    /// A typo in a field name silently doing nothing is the worst failure mode
    /// a workflow file has — `deny_unknown_fields` turns it into a parse error.
    #[test]
    fn an_unknown_field_is_a_parse_error_rather_than_ignored() {
        let err = parse(
            r#"
jobs:
  a:
    vm: { driver: firecracker }
    stpes: [{ run: "true" }]
"#,
        )
        .unwrap_err();
        assert!(matches!(err, WorkflowError::BadJob { .. }), "{err:?}");

        let err = parse(
            r#"
jobs:
  a:
    vm: { driver: firecracker }
    steps:
      - run: "true"
        timeout_minutes: 5
"#,
        )
        .unwrap_err();
        assert!(
            matches!(err, WorkflowError::BadJob { .. }),
            "timeout_minutes is spelled timeout-minutes: {err:?}"
        );
    }

    #[test]
    fn a_workflow_with_no_jobs_is_rejected() {
        assert!(matches!(
            parse("name: empty\njobs: {}\n").unwrap_err(),
            WorkflowError::NoJobs(_)
        ));
    }

    #[test]
    fn a_job_with_no_steps_is_rejected() {
        let err = parse(
            r#"
jobs:
  a:
    vm: { driver: firecracker }
    steps: []
"#,
        )
        .unwrap_err();
        assert_eq!(err, WorkflowError::NoSteps("a".into()));
    }

    #[test]
    fn triggers_parse_in_all_three_spellings() {
        for (yaml, want) in [
            ("on: submit", vec!["submit"]),
            ("on: [submit, schedule]", vec!["submit", "schedule"]),
            ("on:\n  submit:\n    branches: [main]", vec!["submit"]),
        ] {
            let wf = parse(&format!(
                "{yaml}\njobs:\n  a:\n    vm: {{ driver: firecracker }}\n    steps: [{{ run: \"true\" }}]\n"
            ))
            .unwrap();
            assert_eq!(wf.on, want, "{yaml}");
        }
        // Absent `on:` defaults to submit, the only trigger there is.
        let wf =
            parse("jobs:\n  a:\n    vm: { driver: firecracker }\n    steps: [{ run: \"true\" }]\n")
                .unwrap();
        assert_eq!(wf.on, ["submit"]);
    }

    #[test]
    fn step_labels_fall_back_sensibly() {
        let wf = parse(SAMPLE).unwrap();
        let steps = &wf.jobs[0].1.steps;
        assert_eq!(steps[0].label(0), "Build");
        assert_eq!(steps[1].label(1), "ci/upload-artifact");

        let long = Step {
            name: None,
            id: None,
            condition: None,
            uses: None,
            with: BTreeMap::new(),
            run: Some(format!("echo {}", "x".repeat(200))),
            shell: None,
            working_directory: None,
            env: BTreeMap::new(),
            timeout_minutes: None,
            continue_on_error: false,
        };
        let label = long.label(0);
        assert!(label.chars().count() <= 60, "{label}");
        assert!(label.ends_with('…'));
    }
}

#[cfg(test)]
mod repo_workflow {
    use super::*;

    /// This repository's own `.ci/workflows/build.yml` must parse and plan.
    ///
    /// It is the worked example the README points at, so a change that breaks
    /// it breaks the documentation too — and it is the one workflow file that
    /// ships in this repository, which makes it the only one a test can check
    /// without inventing a fixture.
    #[test]
    fn this_repositorys_own_workflow_is_valid() {
        let path = ".ci/workflows/build.yml";
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("{path} must exist and be readable: {e}"));

        let wf =
            Workflow::parse(path, &text).unwrap_or_else(|e| panic!("{path} does not parse: {e}"));
        assert!(wf.on.iter().any(|t| t == "submit"));

        let plan =
            crate::plan::Plan::build(&wf).unwrap_or_else(|e| panic!("{path} does not plan: {e}"));
        assert!(!plan.jobs.is_empty());

        // Every `uses:` step in it must name an action that exists, or the
        // build fails at the step rather than at parse time.
        for job in &plan.jobs {
            for step in &job.steps {
                if let Some(action) = &step.uses {
                    assert_eq!(
                        action, "ci/upload-artifact",
                        "{path} uses {action:?}, which is not a built-in action"
                    );
                }
            }
        }
    }
}
