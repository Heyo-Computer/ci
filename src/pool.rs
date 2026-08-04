//! The warm VM pool, and what busts it.
//!
//! A job would rather inherit a VM that already ran `apt-get install
//! build-essential` and already has a populated `~/.cargo` than boot a fresh
//! one. The question the pool answers is when that inheritance stops being
//! safe.
//!
//! ## The fingerprint
//!
//! ```text
//! sha256( canonical_json(vm block, minus cache_key_files)
//!       ‖ for each path in sorted(cache_key_files):
//!             path ‖ 0x00 ‖ sha256(contents)  — or the ABSENT marker
//!       )[..6]                                → 12 hex characters
//! ```
//!
//! Two decisions in there are worth stating.
//!
//! **`cache_key_files` is removed from the serialized spec before hashing.**
//! Otherwise editing the *list* would change the fingerprint even when every
//! listed file's content is identical, and a workflow that adds a file to the
//! list would rebuild every VM for no reason.
//!
//! **A missing file hashes to an explicit `ABSENT` marker, not to nothing.**
//! Skipping it would make "no `Cargo.lock`" and "an empty `Cargo.lock`"
//! indistinguishable, so *adding* a lockfile later would not bust the pool —
//! which is exactly the moment it most needs busting.
//!
//! ## The pool table is the source of truth, not the daemon
//!
//! `ci_vm_pool` survives a restart on purpose. Without it a crash orphans every
//! VM until its TTL expires, and the next run builds a second pool alongside the
//! one already sitting there.

use crate::vm::VmSpec;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::fmt;
use std::path::Path;

/// Recorded for a `cache_key_files` entry that does not exist.
///
/// A distinct marker rather than an omission, so adding the file later changes
/// the fingerprint.
const ABSENT: &str = "\u{0}ABSENT";

/// Hex characters kept from the digest. 12 gives 48 bits — collision-free in
/// any realistic number of concurrent VM shapes, and short enough to read in a
/// sandbox name.
const FINGERPRINT_LEN: usize = 12;

/// Compute the pool key for a job's VM.
///
/// `workspace` is the materialized checkout; `cache_key_files` are resolved
/// relative to it. They are validated as relative, `..`-free paths when the
/// workflow is parsed, and re-checked here because this function reads files.
pub fn fingerprint(spec: &VmSpec, workspace: &Path) -> Result<String, PoolError> {
    let mut hashable = spec.clone();
    // Removed before serializing — see the module doc.
    hashable.cache_key_files = Vec::new();

    let mut h = Sha256::new();
    let json = serde_json::to_vec(&hashable).map_err(|e| PoolError::Encode(e.to_string()))?;
    h.update(&json);

    // Sorted, so the order the author listed them in does not change the key.
    let mut files = spec.cache_key_files.clone();
    files.sort();
    files.dedup();

    for rel in &files {
        if rel.starts_with('/') || rel.split('/').any(|s| s == "..") {
            return Err(PoolError::EscapingPath(rel.clone()));
        }
        h.update(rel.as_bytes());
        h.update([0u8]);
        match std::fs::read(workspace.join(rel)) {
            Ok(bytes) => {
                let mut fh = Sha256::new();
                fh.update(&bytes);
                h.update(fh.finalize());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => h.update(ABSENT.as_bytes()),
            Err(e) => {
                return Err(PoolError::UnreadableFile {
                    path: rel.clone(),
                    reason: e.to_string(),
                });
            }
        }
    }

    Ok(hex::encode(h.finalize())[..FINGERPRINT_LEN].to_string())
}

/// A VM this orchestrator owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PooledVm {
    pub sandbox_id: String,
    pub runner_hd_id: String,
    pub fingerprint: String,
    pub workflow_id: String,
    pub status: String,
    pub claimed_by_job: Option<String>,
}

impl PooledVm {
    fn from_row(r: &sqlx::postgres::PgRow) -> Self {
        Self {
            sandbox_id: r.get("sandbox_id"),
            runner_hd_id: r.get("runner_hd_id"),
            fingerprint: r.get("fingerprint"),
            workflow_id: r.get("workflow_id"),
            status: r.get("status"),
            claimed_by_job: r.get("claimed_by_job"),
        }
    }
}

#[derive(Clone)]
pub struct Pool {
    db: PgPool,
}

impl Pool {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    /// Take an idle VM on `runner` matching `fingerprint`, or `None`.
    ///
    /// One statement, and that is the point. `FOR UPDATE SKIP LOCKED` makes the
    /// read-and-claim atomic without a transaction the caller has to hold across
    /// a network round trip: two dispatchers racing for the same warm VM each
    /// get a different one, or one gets nothing, but never both the same. A
    /// select-then-update would hand one sandbox to two jobs, and then the
    /// second exec on it fails with `SandboxNotFound` for reasons nothing
    /// explains.
    ///
    /// Most-recently-used first: that VM has the warmest page cache and the most
    /// recently populated build caches.
    pub async fn claim(
        &self,
        runner: &str,
        fingerprint: &str,
        job_id: &str,
    ) -> Result<Option<String>, PoolError> {
        let row = sqlx::query(
            "UPDATE ci_vm_pool
                SET status = 'claimed', claimed_by_job = $3, last_used_at = now()
              WHERE sandbox_id = (
                    SELECT sandbox_id FROM ci_vm_pool
                     WHERE runner_hd_id = $1 AND fingerprint = $2 AND status = 'idle'
                     ORDER BY last_used_at DESC
                     FOR UPDATE SKIP LOCKED
                     LIMIT 1
              )
             RETURNING sandbox_id",
        )
        .bind(runner)
        .bind(fingerprint)
        .bind(job_id)
        .fetch_optional(&self.db)
        .await
        .map_err(PoolError::sql)?;
        Ok(row.map(|r| r.get("sandbox_id")))
    }

    /// Record a freshly created VM as claimed by the job that made it.
    ///
    /// Registered `claimed` rather than `idle`: the creating job is about to use
    /// it, and a window where it is idle is a window where another job takes it.
    pub async fn register(
        &self,
        sandbox_id: &str,
        runner: &str,
        fingerprint: &str,
        workflow_id: &str,
        job_id: &str,
    ) -> Result<(), PoolError> {
        sqlx::query(
            "INSERT INTO ci_vm_pool
                (sandbox_id, runner_hd_id, fingerprint, workflow_id, status, claimed_by_job)
             VALUES ($1,$2,$3,$4,'claimed',$5)
             ON CONFLICT (sandbox_id) DO UPDATE
                SET status='claimed', claimed_by_job=$5, last_used_at=now()",
        )
        .bind(sandbox_id)
        .bind(runner)
        .bind(fingerprint)
        .bind(workflow_id)
        .bind(job_id)
        .execute(&self.db)
        .await
        .map_err(PoolError::sql)?;
        Ok(())
    }

    /// Hand a VM back for the next job with the same fingerprint.
    pub async fn release(&self, sandbox_id: &str) -> Result<(), PoolError> {
        sqlx::query(
            "UPDATE ci_vm_pool
                SET status='idle', claimed_by_job=NULL, last_used_at=now()
              WHERE sandbox_id = $1",
        )
        .bind(sandbox_id)
        .execute(&self.db)
        .await
        .map_err(PoolError::sql)?;
        Ok(())
    }

    /// Forget a VM — it has been destroyed, or is being destroyed.
    pub async fn forget(&self, sandbox_id: &str) -> Result<(), PoolError> {
        sqlx::query("DELETE FROM ci_vm_pool WHERE sandbox_id = $1")
            .bind(sandbox_id)
            .execute(&self.db)
            .await
            .map_err(PoolError::sql)?;
        Ok(())
    }

    pub async fn get(&self, sandbox_id: &str) -> Result<Option<PooledVm>, PoolError> {
        let row = sqlx::query("SELECT * FROM ci_vm_pool WHERE sandbox_id = $1")
            .bind(sandbox_id)
            .fetch_optional(&self.db)
            .await
            .map_err(PoolError::sql)?;
        Ok(row.as_ref().map(PooledVm::from_row))
    }

    pub async fn on_runner(&self, runner: &str) -> Result<Vec<PooledVm>, PoolError> {
        let rows = sqlx::query(
            "SELECT * FROM ci_vm_pool WHERE runner_hd_id = $1 ORDER BY last_used_at DESC",
        )
        .bind(runner)
        .fetch_all(&self.db)
        .await
        .map_err(PoolError::sql)?;
        Ok(rows.iter().map(PooledVm::from_row).collect())
    }

    pub async fn all(&self) -> Result<Vec<PooledVm>, PoolError> {
        let rows = sqlx::query("SELECT * FROM ci_vm_pool ORDER BY runner_hd_id, last_used_at DESC")
            .fetch_all(&self.db)
            .await
            .map_err(PoolError::sql)?;
        Ok(rows.iter().map(PooledVm::from_row).collect())
    }

    /// Idle VMs whose fingerprint is no longer wanted, or which have sat unused
    /// too long.
    ///
    /// **Scoped to `runners`, and that scope is not optional.** Jobs are sharded
    /// per runner precisely so several orchestrators can run at once, each
    /// owning a disjoint set of hosts. An unscoped sweep would let instance A
    /// mark instance B's VMs `draining` — taking them out of B's pool — and then
    /// try to destroy machines it has no tunnel to. The rows would be stranded
    /// in `draining` forever and the VMs would leak until their TTL.
    ///
    /// Returns them rather than destroying them: destroying needs a tunnel to
    /// the right runner, and the row should only go away once the daemon
    /// confirms. Marking `draining` first is what stops a concurrent `claim`
    /// from handing out a VM that is about to be killed.
    pub async fn take_for_sweep(
        &self,
        runners: &[String],
        live_fingerprints: &[String],
        idle_for_secs: i64,
    ) -> Result<Vec<PooledVm>, PoolError> {
        if runners.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "UPDATE ci_vm_pool
                SET status = 'draining'
              WHERE sandbox_id IN (
                    SELECT sandbox_id FROM ci_vm_pool
                     WHERE status = 'idle'
                       AND runner_hd_id = ANY($1)
                       AND (NOT (fingerprint = ANY($2))
                            OR last_used_at < now() - make_interval(secs => $3))
                     FOR UPDATE SKIP LOCKED
              )
             RETURNING *",
        )
        .bind(runners)
        .bind(live_fingerprints)
        .bind(idle_for_secs as f64)
        .fetch_all(&self.db)
        .await
        .map_err(PoolError::sql)?;
        Ok(rows.iter().map(PooledVm::from_row).collect())
    }

    /// Release VMs held by a job that is no longer running.
    ///
    /// Run at startup: a crash leaves rows `claimed` by a job that died with it,
    /// and without this those VMs are never handed out again — the pool leaks
    /// its whole capacity one restart at a time.
    ///
    /// Scoped to `runners` for the same reason [`take_for_sweep`] is. The job
    /// status check alone would in fact keep another instance's *running* work
    /// safe, but relying on that means a bug in status bookkeeping turns into
    /// two jobs sharing one sandbox — and the symptom of that is a
    /// `SandboxNotFound` from a exec that looks like a daemon fault.
    ///
    /// [`take_for_sweep`]: Self::take_for_sweep
    pub async fn release_orphans(&self, runners: &[String]) -> Result<u64, PoolError> {
        if runners.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query(
            "UPDATE ci_vm_pool p
                SET status='idle', claimed_by_job=NULL
              WHERE p.status = 'claimed'
                AND p.runner_hd_id = ANY($1)
                AND (p.claimed_by_job IS NULL
                     OR NOT EXISTS (
                        SELECT 1 FROM ci_job j
                         WHERE j.id = p.claimed_by_job
                           AND j.status IN ('pending','queued','running')))",
        )
        .bind(runners)
        .execute(&self.db)
        .await
        .map_err(PoolError::sql)?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub enum PoolError {
    Encode(String),
    EscapingPath(String),
    UnreadableFile { path: String, reason: String },
    Sql(String),
}

impl PoolError {
    fn sql(e: sqlx::Error) -> Self {
        Self::Sql(e.to_string())
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(e) => write!(f, "could not serialize a vm spec for hashing: {e}"),
            Self::EscapingPath(p) => write!(
                f,
                "cache_key_files entry {p:?} escapes the checkout; it must be a \
                 relative path with no `..` segments"
            ),
            Self::UnreadableFile { path, reason } => write!(
                f,
                "could not read cache_key_files entry {path:?}: {reason}. A missing \
                 file is fine and busts the pool when it appears; this one exists \
                 but could not be read."
            ),
            Self::Sql(e) => write!(f, "database error: {e}"),
        }
    }
}

impl std::error::Error for PoolError {}

#[cfg(test)]
mod tests {
    use super::*;
    use heyo_sdk::{SandboxDriver, SandboxSize};
    use std::collections::BTreeMap;

    fn spec() -> VmSpec {
        VmSpec {
            driver: SandboxDriver::Firecracker,
            image: Some("ubuntu:24.04".into()),
            size_class: Some(SandboxSize::Medium),
            disk_size_gb: Some(20),
            working_directory: Some("/workspace".into()),
            env_vars: BTreeMap::new(),
            setup_hooks: vec!["apt-get install -y build-essential".into()],
            cache_key_files: vec![],
            reuse: true,
            ttl_seconds: None,
        }
    }

    fn workspace() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ci-fp-{}", crate::vm::new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_fingerprint_is_twelve_hex_characters() {
        let ws = workspace();
        let fp = fingerprint(&spec(), &ws).unwrap();
        assert_eq!(fp.len(), 12);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
        std::fs::remove_dir_all(&ws).ok();
    }

    /// The same spec must produce the same key in every process and on every
    /// restart, or the pool is never hit twice.
    #[test]
    fn the_same_spec_hashes_the_same_way_every_time() {
        let ws = workspace();
        let a = fingerprint(&spec(), &ws).unwrap();
        for _ in 0..10 {
            assert_eq!(fingerprint(&spec(), &ws).unwrap(), a);
        }
        std::fs::remove_dir_all(&ws).ok();
    }

    /// Every field of the vm block is part of the machine, so every one of them
    /// has to bust the pool.
    #[test]
    fn any_change_to_the_vm_block_busts_the_pool() {
        let ws = workspace();
        let base = fingerprint(&spec(), &ws).unwrap();

        let mut s = spec();
        s.image = Some("ubuntu:22.04".into());
        assert_ne!(fingerprint(&s, &ws).unwrap(), base, "image");

        let mut s = spec();
        s.size_class = Some(SandboxSize::Large);
        assert_ne!(fingerprint(&s, &ws).unwrap(), base, "size_class");

        let mut s = spec();
        s.driver = SandboxDriver::Kvm;
        assert_ne!(fingerprint(&s, &ws).unwrap(), base, "driver");

        let mut s = spec();
        s.setup_hooks.push("apt-get install -y cmake".into());
        assert_ne!(fingerprint(&s, &ws).unwrap(), base, "setup_hooks");

        let mut s = spec();
        s.env_vars.insert("RUSTFLAGS".into(), "-C lto".into());
        assert_ne!(fingerprint(&s, &ws).unwrap(), base, "env_vars");

        let mut s = spec();
        s.disk_size_gb = Some(40);
        assert_ne!(fingerprint(&s, &ws).unwrap(), base, "disk_size_gb");

        std::fs::remove_dir_all(&ws).ok();
    }

    /// The headline behaviour: touch a declared file's *contents* and the next
    /// run gets a fresh VM.
    #[test]
    fn changing_a_cache_key_files_content_busts_the_pool() {
        let ws = workspace();
        std::fs::write(ws.join("Cargo.lock"), "version = 3\n").unwrap();
        let mut s = spec();
        s.cache_key_files = vec!["Cargo.lock".into()];

        let before = fingerprint(&s, &ws).unwrap();
        std::fs::write(ws.join("Cargo.lock"), "version = 4\n").unwrap();
        let after = fingerprint(&s, &ws).unwrap();
        assert_ne!(before, after);

        // And putting the content back returns the original key, so a revert
        // reuses the VM that was already warm for it.
        std::fs::write(ws.join("Cargo.lock"), "version = 3\n").unwrap();
        assert_eq!(fingerprint(&s, &ws).unwrap(), before);
        std::fs::remove_dir_all(&ws).ok();
    }

    /// Reordering the list is not a change to the machine.
    #[test]
    fn the_order_of_cache_key_files_does_not_matter() {
        let ws = workspace();
        std::fs::write(ws.join("a.lock"), "a").unwrap();
        std::fs::write(ws.join("b.lock"), "b").unwrap();

        let mut s1 = spec();
        s1.cache_key_files = vec!["a.lock".into(), "b.lock".into()];
        let mut s2 = spec();
        s2.cache_key_files = vec!["b.lock".into(), "a.lock".into()];

        assert_eq!(
            fingerprint(&s1, &ws).unwrap(),
            fingerprint(&s2, &ws).unwrap()
        );
        std::fs::remove_dir_all(&ws).ok();
    }

    /// Two files with swapped contents must not hash the same — the path is
    /// mixed in, so `a=x, b=y` differs from `a=y, b=x`.
    #[test]
    fn swapping_contents_between_two_files_is_a_change() {
        let ws = workspace();
        let mut s = spec();
        s.cache_key_files = vec!["a.lock".into(), "b.lock".into()];

        std::fs::write(ws.join("a.lock"), "x").unwrap();
        std::fs::write(ws.join("b.lock"), "y").unwrap();
        let before = fingerprint(&s, &ws).unwrap();

        std::fs::write(ws.join("a.lock"), "y").unwrap();
        std::fs::write(ws.join("b.lock"), "x").unwrap();
        assert_ne!(fingerprint(&s, &ws).unwrap(), before);
        std::fs::remove_dir_all(&ws).ok();
    }

    /// The reason a missing file gets an explicit marker: adding the file must
    /// bust the pool, and it only can if "absent" and "empty" differ.
    #[test]
    fn adding_a_previously_missing_file_busts_the_pool() {
        let ws = workspace();
        let mut s = spec();
        s.cache_key_files = vec!["rust-toolchain.toml".into()];

        let missing = fingerprint(&s, &ws).unwrap();
        std::fs::write(ws.join("rust-toolchain.toml"), "").unwrap();
        let empty = fingerprint(&s, &ws).unwrap();
        assert_ne!(missing, empty, "absent must differ from present-but-empty");

        std::fs::write(ws.join("rust-toolchain.toml"), "[toolchain]\n").unwrap();
        assert_ne!(fingerprint(&s, &ws).unwrap(), empty);
        std::fs::remove_dir_all(&ws).ok();
    }

    /// Editing the *list* while every listed file's content stays the same must
    /// not rebuild the VM — that is why the list is stripped before hashing.
    #[test]
    fn listing_a_file_whose_content_is_unchanged_is_still_a_change_but_reordering_is_not() {
        let ws = workspace();
        std::fs::write(ws.join("a.lock"), "a").unwrap();

        let none = fingerprint(&spec(), &ws).unwrap();
        let mut s = spec();
        s.cache_key_files = vec!["a.lock".into()];
        let listed = fingerprint(&s, &ws).unwrap();
        // Declaring a file *does* change the key, because the pool now depends
        // on something it did not before. What must not change it is the list's
        // order or duplicates.
        assert_ne!(none, listed);

        let mut dup = spec();
        dup.cache_key_files = vec!["a.lock".into(), "a.lock".into()];
        assert_eq!(
            fingerprint(&dup, &ws).unwrap(),
            listed,
            "a duplicate entry is not a change"
        );
        std::fs::remove_dir_all(&ws).ok();
    }

    #[test]
    fn an_escaping_cache_key_path_is_refused_even_here() {
        let ws = workspace();
        let mut s = spec();
        s.cache_key_files = vec!["../../etc/passwd".into()];
        assert!(matches!(
            fingerprint(&s, &ws),
            Err(PoolError::EscapingPath(_))
        ));
        std::fs::remove_dir_all(&ws).ok();
    }

    /// The key goes into a sandbox name, which the daemon parses as hex to
    /// derive a tap subnet.
    #[test]
    fn a_fingerprint_is_safe_in_a_sandbox_name() {
        let ws = workspace();
        let fp = fingerprint(&spec(), &ws).unwrap();
        let name = crate::vm::sandbox_name("build", &fp, 7);
        assert!(name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'));
        std::fs::remove_dir_all(&ws).ok();
    }

    // ---- integration ----------------------------------------------------
    //
    //   CI_TEST_DATABASE_URL=... cargo test -- --ignored pool::

    async fn test_pool() -> (Pool, crate::store::Store) {
        let url = std::env::var("CI_TEST_DATABASE_URL").expect("CI_TEST_DATABASE_URL");
        let dir = std::env::temp_dir().join(format!("ci-pool-logs-{}", crate::vm::new_id()));
        let store = crate::store::Store::connect(&url, dir).await.unwrap();
        store
            .migrate(Path::new("migrations"))
            .await
            .expect("migrations");
        let pool = Pool::new(store.pool().clone());
        (pool, store)
    }

    /// A distinct runner per test so they never contend.
    fn runner_id() -> String {
        format!("hd-{}", crate::vm::new_id().replace('-', ""))
    }

    /// `sandbox_id` is the pool's primary key and therefore global, while the
    /// runner id is per-test. Deriving one from the other keeps concurrent test
    /// runs from overwriting each other's rows.
    fn sb(runner: &str, name: &str) -> String {
        format!("sb-{name}-{}", runner.trim_start_matches("hd-"))
    }

    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn an_idle_vm_with_a_matching_fingerprint_is_reused() {
        let (pool, _s) = test_pool().await;
        let runner = runner_id();

        pool.register(&sb(&runner, "1"), &runner, "fp-a", "wf", "job-1")
            .await
            .unwrap();
        // Claimed by its creator, so nobody else can take it yet.
        assert_eq!(pool.claim(&runner, "fp-a", "job-2").await.unwrap(), None);

        pool.release(&sb(&runner, "1")).await.unwrap();
        assert_eq!(
            pool.claim(&runner, "fp-a", "job-2").await.unwrap(),
            Some(sb(&runner, "1")),
            "the next job with the same fingerprint inherits it"
        );
        // And now it is taken again.
        assert_eq!(pool.claim(&runner, "fp-a", "job-3").await.unwrap(), None);
    }

    /// The busting behaviour, at the pool level: a different fingerprint must
    /// not match a warm VM.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn a_different_fingerprint_does_not_match() {
        let (pool, _s) = test_pool().await;
        let runner = runner_id();
        pool.register(&sb(&runner, "2"), &runner, "fp-a", "wf", "job-1")
            .await
            .unwrap();
        pool.release(&sb(&runner, "2")).await.unwrap();
        assert_eq!(pool.claim(&runner, "fp-b", "job-2").await.unwrap(), None);
        assert_eq!(
            pool.claim(&runner, "fp-a", "job-2").await.unwrap(),
            Some(sb(&runner, "2"))
        );
    }

    /// The pool is host-local: a warm VM on one host is no use to a job pinned
    /// to another.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn a_warm_vm_on_another_runner_is_not_reused() {
        let (pool, _s) = test_pool().await;
        let a = runner_id();
        let b = runner_id();
        pool.register(&sb(&a, "3"), &a, "fp-a", "wf", "job-1")
            .await
            .unwrap();
        pool.release(&sb(&a, "3")).await.unwrap();
        assert_eq!(pool.claim(&b, "fp-a", "job-2").await.unwrap(), None);
    }

    /// Two dispatchers racing must never be handed the same sandbox — the
    /// second exec on it would fail with `SandboxNotFound`.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn concurrent_claims_never_hand_out_the_same_vm() {
        let (pool, _s) = test_pool().await;
        let runner = runner_id();
        for i in 0..4 {
            let id = format!("sb-race-{i}-{}", crate::vm::new_id());
            pool.register(&id, &runner, "fp-race", "wf", "job-0")
                .await
                .unwrap();
            pool.release(&id).await.unwrap();
        }

        // Eight claimers for four VMs.
        let mut handles = Vec::new();
        for i in 0..8 {
            let p = pool.clone();
            let r = runner.clone();
            handles.push(tokio::spawn(async move {
                p.claim(&r, "fp-race", &format!("job-{i}")).await.unwrap()
            }));
        }
        let mut claimed = Vec::new();
        for h in handles {
            if let Some(id) = h.await.unwrap() {
                claimed.push(id);
            }
        }

        assert_eq!(claimed.len(), 4, "exactly the four available VMs");
        let unique: std::collections::BTreeSet<_> = claimed.iter().collect();
        assert_eq!(unique.len(), 4, "no VM was handed out twice: {claimed:?}");
    }

    /// A crash leaves rows claimed by a job that is no longer running; without
    /// reclaiming them the pool leaks its capacity one restart at a time.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn orphaned_claims_are_released_at_startup() {
        let (pool, _s) = test_pool().await;
        let runner = runner_id();
        // `job-gone` does not exist in ci_job at all.
        pool.register(&sb(&runner, "orphan"), &runner, "fp-o", "wf", "job-gone")
            .await
            .unwrap();
        assert_eq!(pool.claim(&runner, "fp-o", "job-x").await.unwrap(), None);

        let released = pool
            .release_orphans(std::slice::from_ref(&runner))
            .await
            .unwrap();
        assert_eq!(released, 1, "only this runner's orphan");
        assert_eq!(
            pool.claim(&runner, "fp-o", "job-x").await.unwrap(),
            Some(sb(&runner, "orphan")),
            "a VM held by a dead job must come back"
        );
    }

    /// Sweeping marks VMs `draining` so a concurrent claim cannot take one that
    /// is about to be destroyed.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn sweeping_takes_stale_fingerprints_out_of_circulation() {
        let (pool, _s) = test_pool().await;
        let runner = runner_id();
        pool.register(&sb(&runner, "live"), &runner, "fp-live", "wf", "j")
            .await
            .unwrap();
        pool.register(&sb(&runner, "dead"), &runner, "fp-dead", "wf", "j")
            .await
            .unwrap();
        pool.release(&sb(&runner, "live")).await.unwrap();
        pool.release(&sb(&runner, "dead")).await.unwrap();

        let swept = pool
            .take_for_sweep(
                std::slice::from_ref(&runner),
                &["fp-live".to_string()],
                86_400,
            )
            .await
            .unwrap();
        let ids: Vec<&str> = swept.iter().map(|v| v.sandbox_id.as_str()).collect();
        assert!(ids.contains(&sb(&runner, "dead").as_str()), "{ids:?}");
        assert!(
            !ids.contains(&sb(&runner, "live").as_str()),
            "a wanted fingerprint survives"
        );

        // Draining, so no longer claimable.
        assert_eq!(pool.claim(&runner, "fp-dead", "j2").await.unwrap(), None);
        // And the wanted one is still there.
        assert_eq!(
            pool.claim(&runner, "fp-live", "j2").await.unwrap(),
            Some(sb(&runner, "live"))
        );

        pool.forget(&sb(&runner, "dead")).await.unwrap();
        assert!(pool.get(&sb(&runner, "dead")).await.unwrap().is_none());
    }

    /// An idle VM nobody has wanted for a long time is swept even when its
    /// fingerprint is still current — otherwise a workflow that stops running
    /// keeps a machine's worth of VMs alive forever.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn a_long_idle_vm_is_swept_even_if_its_fingerprint_is_current() {
        let (pool, _s) = test_pool().await;
        let runner = runner_id();
        pool.register(&sb(&runner, "stale"), &runner, "fp-cur", "wf", "j")
            .await
            .unwrap();
        pool.release(&sb(&runner, "stale")).await.unwrap();

        // Nothing swept while it is fresh.
        assert!(
            pool.take_for_sweep(std::slice::from_ref(&runner), &["fp-cur".to_string()], 3600)
                .await
                .unwrap()
                .is_empty()
        );
        // Zero idle window: everything idle is stale.
        let swept = pool
            .take_for_sweep(std::slice::from_ref(&runner), &["fp-cur".to_string()], 0)
            .await
            .unwrap();
        assert!(swept.iter().any(|v| v.sandbox_id == sb(&runner, "stale")));
        pool.forget(&sb(&runner, "stale")).await.unwrap();
    }
}
