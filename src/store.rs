//! Postgres persistence for runs, jobs, steps, artifacts and the VM pool.
//!
//! Runtime `sqlx::query()` with `.bind()`, not the `query!` macros — so the
//! crate compiles with no database reachable, which is what heyosecret does and
//! what makes a CI build of this CI system possible.
//!
//! **Step logs are not in here.** A row holds a path and a byte count; the bytes
//! are appended to a file under `CI_LOG_DIR`. A build log is megabytes, and
//! putting it in a column means every `SELECT * FROM ci_step` for a status page
//! drags all of it across the wire.
//!
//! Migrations are applied by re-executing `migrations/*.sql` in filename order
//! on every startup, with no tracking table — heyosecret's approach. It puts one
//! obligation on the SQL (every statement idempotent) in exchange for removing a
//! whole class of "the migration table disagrees with the schema" incidents.

use crate::plan::{JobPlan, Plan};
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Advisory-lock key serializing migrations across processes. An arbitrary
/// constant; it only has to be the same in every instance and unlikely to
/// collide with another application sharing the database.
const MIGRATION_LOCK_KEY: i64 = 0x0c19_3a7e;

/// How many times to retry a migration that lost a lock race with a running
/// instance. Contention is transient — the other side is a build finishing —
/// so a few short retries beat both failing the start and waiting forever.
const MIGRATION_ATTEMPTS: u32 = 5;

/// Whether a migration failure is the kind that retrying fixes.
///
/// Matched on the message because `sqlx::Error::Database` only exposes the
/// SQLSTATE through a downcast to the driver's error type, and both of these
/// arrive as plain database errors. The two codes are `40P01` (deadlock
/// detected) and `55P03` (lock not available).
fn is_lock_contention(e: &StoreError) -> bool {
    let text = e.to_string();
    text.contains("deadlock detected")
        || text.contains("lock timeout")
        || text.contains("canceling statement due to lock timeout")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Queued,
    Running,
    Success,
    Failure,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Success | Self::Failure | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Queued,
    Running,
    Success,
    Failure,
    Skipped,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failure | Self::Skipped | Self::Cancelled
        )
    }

    /// What `needs.<job>.result` reports. A skipped dependency is not a failure
    /// — GitHub's `needs` context reports it as `skipped`, and a downstream
    /// `if:` may legitimately want to run anyway.
    pub fn result_name(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
            _ => "pending",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepStatus {
    Pending,
    Running,
    Success,
    Failure,
    Skipped,
}

impl StepStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub id: String,
    pub workflow_id: String,
    pub workflow_path: String,
    pub workflow_name: Option<String>,
    pub repo_url: String,
    pub git_ref: String,
    pub sha: String,
    pub actor_email: Option<String>,
    pub status: String,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl Run {
    fn from_row(r: &PgRow) -> Self {
        Self {
            id: r.get("id"),
            workflow_id: r.get("workflow_id"),
            workflow_path: r.get("workflow_path"),
            workflow_name: r.get("workflow_name"),
            repo_url: r.get("repo_url"),
            git_ref: r.get("git_ref"),
            sha: r.get("sha"),
            actor_email: r.get("actor_email"),
            status: r.get("status"),
            error: r.get("error"),
            created_at: r.get("created_at"),
            started_at: r.get("started_at"),
            finished_at: r.get("finished_at"),
        }
    }

    /// How long the run took, or has been going.
    pub fn duration(&self) -> Option<Duration> {
        let start = self.started_at?;
        let end = self.finished_at.unwrap_or_else(Utc::now);
        (end - start).to_std().ok()
    }
}

#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: String,
    pub run_id: String,
    pub job_key: String,
    pub base_id: String,
    pub display: String,
    pub network: Option<String>,
    pub runner_hd_id: Option<String>,
    pub fingerprint: Option<String>,
    pub sandbox_id: Option<String>,
    pub status: String,
    pub attempt: i32,
    pub matrix: serde_json::Value,
    pub outputs: serde_json::Value,
    /// The expanded `JobPlan` this job runs. The queue message carries only
    /// ids, so this is what a redelivery executes.
    pub plan: serde_json::Value,
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl JobRow {
    fn from_row(r: &PgRow) -> Self {
        Self {
            id: r.get("id"),
            run_id: r.get("run_id"),
            job_key: r.get("job_key"),
            base_id: r.get("base_id"),
            display: r.get("display"),
            network: r.get("network"),
            runner_hd_id: r.get("runner_hd_id"),
            fingerprint: r.get("fingerprint"),
            sandbox_id: r.get("sandbox_id"),
            status: r.get("status"),
            attempt: r.get("attempt"),
            matrix: r.get("matrix"),
            outputs: r.get("outputs"),
            plan: r.get("plan"),
            error: r.get("error"),
            started_at: r.get("started_at"),
            finished_at: r.get("finished_at"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StepRow {
    pub id: String,
    pub job_id: String,
    pub idx: i32,
    pub name: String,
    pub uses: Option<String>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub operation_id: Option<String>,
    pub log_path: Option<String>,
    pub log_bytes: i64,
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl StepRow {
    fn from_row(r: &PgRow) -> Self {
        Self {
            id: r.get("id"),
            job_id: r.get("job_id"),
            idx: r.get("idx"),
            name: r.get("name"),
            uses: r.get("uses"),
            status: r.get("status"),
            exit_code: r.get("exit_code"),
            operation_id: r.get("operation_id"),
            log_path: r.get("log_path"),
            log_bytes: r.get("log_bytes"),
            error: r.get("error"),
            started_at: r.get("started_at"),
            finished_at: r.get("finished_at"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactRow {
    pub name: String,
    pub sink: String,
    pub digest: Option<String>,
    pub size_bytes: i64,
    pub uri: String,
}

/// What a run is being started for.
#[derive(Debug, Clone, Default)]
pub struct RunRequest {
    pub workflow_id: String,
    pub repo_url: String,
    pub git_ref: String,
    pub sha: String,
    pub before_sha: String,
    pub actor_subject: Option<String>,
    pub actor_email: Option<String>,
    pub source: String,
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    log_dir: PathBuf,
}

impl Store {
    pub async fn connect(database_url: &str, log_dir: PathBuf) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            // Fail fast at startup rather than after the default 30s: a wrong
            // `CI_DATABASE_URL` should look like a configuration error, not a
            // hang.
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await
            .map_err(|e| StoreError::Connect(e.to_string()))?;
        tokio::fs::create_dir_all(&log_dir)
            .await
            .map_err(|e| StoreError::LogDir {
                path: log_dir.clone(),
                reason: e.to_string(),
            })?;
        Ok(Self { pool, log_dir })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply `migrations/*.sql` in filename order.
    ///
    /// Every file is re-executed on every startup, so each statement must be
    /// idempotent. There is no tracking table by design — see the module doc.
    ///
    /// **Serialized by a Postgres advisory lock**, and that is not belt and
    /// braces. `CREATE TABLE IF NOT EXISTS` is *not* concurrency-safe: two
    /// sessions both find the table absent, both proceed, and the loser fails
    /// with `duplicate key value violates unique constraint
    /// "pg_type_typname_nsp_index"` from the system catalogue. Since jobs are
    /// sharded per runner precisely so several orchestrators can run at once,
    /// two of them starting together is a normal event rather than a rare race.
    ///
    /// **And bounded by a lock timeout, with retries.** The advisory lock keeps
    /// migrators away from each other, but not away from *running* instances:
    /// `ALTER TABLE … ADD COLUMN` needs `ACCESS EXCLUSIVE` on a table that a
    /// live dispatcher is inserting into. Without a timeout that wait is
    /// unbounded — a rolling deploy would hang a starting instance behind a long
    /// build — and Postgres reports some of those cycles as `deadlock detected`
    /// rather than waiting at all. Failing fast and retrying turns both into a
    /// few seconds of startup delay.
    pub async fn migrate(&self, dir: &Path) -> Result<(), StoreError> {
        let mut last: Option<StoreError> = None;
        for attempt in 1..=MIGRATION_ATTEMPTS {
            match self.migrate_once(dir).await {
                Ok(()) => return Ok(()),
                Err(e) if is_lock_contention(&e) && attempt < MIGRATION_ATTEMPTS => {
                    tracing::warn!(
                        "migration attempt {attempt} hit lock contention with a running \
                         instance, retrying: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
                    last = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| StoreError::Sql("migration retries exhausted".into())))
    }

    async fn migrate_once(&self, dir: &Path) -> Result<(), StoreError> {
        let mut conn = self.pool.acquire().await.map_err(StoreError::sql)?;

        // Bounded so a migration never blocks indefinitely behind a live
        // dispatcher's inserts. Session-scoped, and the connection is returned
        // to the pool afterwards, so it is reset explicitly below.
        sqlx::query("SET lock_timeout = '5s'")
            .execute(&mut *conn)
            .await
            .map_err(StoreError::sql)?;

        // The lock is held on this one connection and released explicitly
        // below; a dropped connection also releases it, so a panicking migrator
        // cannot wedge every future start.
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(MIGRATION_LOCK_KEY)
            .execute(&mut *conn)
            .await
            .map_err(StoreError::sql)?;

        let result = self.run_migrations(dir, &mut conn).await;

        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(MIGRATION_LOCK_KEY)
            .execute(&mut *conn)
            .await;
        // Undo the session setting; this connection goes back into the pool and
        // a 5s lock timeout on ordinary queries would be a surprising default.
        let _ = sqlx::query("SET lock_timeout = DEFAULT")
            .execute(&mut *conn)
            .await;
        result
    }

    async fn run_migrations(
        &self,
        dir: &Path,
        conn: &mut sqlx::PgConnection,
    ) -> Result<(), StoreError> {
        let mut entries = Vec::new();
        let mut rd = tokio::fs::read_dir(dir)
            .await
            .map_err(|e| StoreError::Migrations {
                path: dir.to_path_buf(),
                reason: e.to_string(),
            })?;
        while let Some(entry) = rd.next_entry().await.map_err(|e| StoreError::Migrations {
            path: dir.to_path_buf(),
            reason: e.to_string(),
        })? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("sql") {
                entries.push(path);
            }
        }
        entries.sort();

        if entries.is_empty() {
            return Err(StoreError::Migrations {
                path: dir.to_path_buf(),
                reason: "no .sql files found".to_string(),
            });
        }

        for path in entries {
            let sql =
                tokio::fs::read_to_string(&path)
                    .await
                    .map_err(|e| StoreError::Migrations {
                        path: path.clone(),
                        reason: e.to_string(),
                    })?;
            sqlx::raw_sql(&sql)
                .execute(&mut *conn)
                .await
                .map_err(|e| StoreError::Migrations {
                    path: path.clone(),
                    reason: e.to_string(),
                })?;
            tracing::debug!("applied migration {}", path.display());
        }
        Ok(())
    }

    /// Insert a run and every job of its plan, in one transaction.
    ///
    /// All-or-nothing on purpose: a run whose jobs half-exist is worse than no
    /// run, because the dashboard shows a DAG that can never complete and
    /// nothing owns fixing it.
    pub async fn create_run(
        &self,
        run_id: &str,
        req: &RunRequest,
        plan: &Plan,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(StoreError::sql)?;

        sqlx::query(
            "INSERT INTO ci_run (id, workflow_id, workflow_path, workflow_name, repo_url,
                                 git_ref, sha, before_sha, actor_subject, actor_email,
                                 source, status)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'queued')",
        )
        .bind(run_id)
        .bind(&req.workflow_id)
        .bind(&plan.workflow_path)
        .bind(&plan.workflow_name)
        .bind(&req.repo_url)
        .bind(&req.git_ref)
        .bind(&req.sha)
        .bind(&req.before_sha)
        .bind(&req.actor_subject)
        .bind(&req.actor_email)
        .bind(if req.source.is_empty() {
            "submit"
        } else {
            &req.source
        })
        .execute(&mut *tx)
        .await
        .map_err(StoreError::sql)?;

        for job in &plan.jobs {
            let matrix = serde_json::to_value(&job.matrix).unwrap_or(serde_json::json!({}));
            let plan_json = serde_json::to_value(job).unwrap_or(serde_json::json!({}));
            sqlx::query(
                "INSERT INTO ci_job (id, run_id, job_key, base_id, display, network,
                                     fingerprint, status, matrix, plan)
                 VALUES ($1,$2,$3,$4,$5,$6,NULL,'pending',$7,$8)",
            )
            .bind(job_id(run_id, &job.key))
            .bind(run_id)
            .bind(&job.key)
            .bind(&job.base_id)
            .bind(&job.display)
            .bind(&job.target.network)
            .bind(&matrix)
            .bind(&plan_json)
            .execute(&mut *tx)
            .await
            .map_err(StoreError::sql)?;
        }

        tx.commit().await.map_err(StoreError::sql)?;
        Ok(())
    }

    pub async fn get_run(&self, run_id: &str) -> Result<Option<Run>, StoreError> {
        let row = sqlx::query("SELECT * FROM ci_run WHERE id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(StoreError::sql)?;
        Ok(row.as_ref().map(Run::from_row))
    }

    pub async fn recent_runs(&self, limit: i64) -> Result<Vec<Run>, StoreError> {
        let rows = sqlx::query("SELECT * FROM ci_run ORDER BY created_at DESC LIMIT $1")
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::sql)?;
        Ok(rows.iter().map(Run::from_row).collect())
    }

    pub async fn set_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        // The timestamps are set by the same statement that sets the status, so
        // a run can never be `running` with no `started_at` for a reader to trip
        // over.
        sqlx::query(
            "UPDATE ci_run
                SET status = $2,
                    error = COALESCE($3, error),
                    started_at = CASE WHEN $2 = 'running' AND started_at IS NULL
                                      THEN now() ELSE started_at END,
                    finished_at = CASE WHEN $2 IN ('success','failure','cancelled')
                                       THEN now() ELSE finished_at END
              WHERE id = $1",
        )
        .bind(run_id)
        .bind(status.as_str())
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(())
    }

    /// Recompute a run's status from its jobs, and return it.
    ///
    /// The run is the roll-up of its jobs rather than a separately maintained
    /// value, so the two cannot disagree — which is exactly what happens when
    /// a crash lands between "last job finished" and "mark the run done".
    pub async fn roll_up_run(&self, run_id: &str) -> Result<RunStatus, StoreError> {
        let rows = sqlx::query("SELECT status FROM ci_job WHERE run_id = $1")
            .bind(run_id)
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::sql)?;

        let statuses: Vec<String> = rows.iter().map(|r| r.get::<String, _>("status")).collect();
        let status = if statuses.is_empty() {
            RunStatus::Success
        } else if statuses.iter().any(|s| s == "cancelled") {
            RunStatus::Cancelled
        } else if statuses.iter().any(|s| s == "failure") {
            // A failure is terminal for the run even while other jobs are still
            // going: the answer to "did this commit pass" is already no.
            RunStatus::Failure
        } else if statuses
            .iter()
            .all(|s| matches!(s.as_str(), "success" | "skipped"))
        {
            RunStatus::Success
        } else {
            RunStatus::Running
        };

        self.set_run_status(run_id, status, None).await?;
        Ok(status)
    }

    pub async fn jobs_of(&self, run_id: &str) -> Result<Vec<JobRow>, StoreError> {
        let rows =
            sqlx::query("SELECT * FROM ci_job WHERE run_id = $1 ORDER BY created_at, job_key")
                .bind(run_id)
                .fetch_all(&self.pool)
                .await
                .map_err(StoreError::sql)?;
        Ok(rows.iter().map(JobRow::from_row).collect())
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Option<JobRow>, StoreError> {
        let row = sqlx::query("SELECT * FROM ci_job WHERE id = $1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(StoreError::sql)?;
        Ok(row.as_ref().map(JobRow::from_row))
    }

    /// Move a job to `running` and record where it landed.
    ///
    /// Returns false when the job was already terminal, which is how a
    /// redelivery of work that finished just before the ack is dropped instead
    /// of run twice.
    pub async fn start_job(
        &self,
        job_id: &str,
        runner_hd_id: &str,
        sandbox_id: &str,
        fingerprint: &str,
        attempt: i32,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE ci_job
                SET status = 'running', runner_hd_id = $2, sandbox_id = $3,
                    fingerprint = $4, attempt = $5,
                    started_at = COALESCE(started_at, now())
              WHERE id = $1
                AND status NOT IN ('success','failure','skipped','cancelled')",
        )
        .bind(job_id)
        .bind(runner_hd_id)
        .bind(sandbox_id)
        .bind(fingerprint)
        .bind(attempt)
        .execute(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(result.rows_affected() > 0)
    }

    /// Move a job from `pending` to `queued`.
    ///
    /// Returns false when it was not pending, which is what stops a second
    /// scheduler pass from publishing the same job twice. The queue's own
    /// `Nats-Msg-Id` dedup is the belt; this is the braces, and it also keeps
    /// the row's status honest.
    pub async fn queue_job(&self, job_id: &str) -> Result<bool, StoreError> {
        let result =
            sqlx::query("UPDATE ci_job SET status = 'queued' WHERE id = $1 AND status = 'pending'")
                .bind(job_id)
                .execute(&self.pool)
                .await
                .map_err(StoreError::sql)?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn set_job_status(
        &self,
        job_id: &str,
        status: JobStatus,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE ci_job
                SET status = $2,
                    error = COALESCE($3, error),
                    finished_at = CASE WHEN $2 IN ('success','failure','skipped','cancelled')
                                       THEN now() ELSE finished_at END
              WHERE id = $1",
        )
        .bind(job_id)
        .bind(status.as_str())
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(())
    }

    pub async fn set_job_outputs(
        &self,
        job_id: &str,
        outputs: &serde_json::Value,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE ci_job SET outputs = $2 WHERE id = $1")
            .bind(job_id)
            .bind(outputs)
            .execute(&self.pool)
            .await
            .map_err(StoreError::sql)?;
        Ok(())
    }

    /// The `needs` context for a run: `{ "<base_id>": { "result": …, "outputs": … } }`.
    ///
    /// Keyed on `base_id`, and a base with several matrix cells collapses to the
    /// worst result among them — `needs.build.result` has to mean "did build
    /// pass", and it did not if any cell failed.
    pub async fn needs_context(&self, run_id: &str) -> Result<serde_json::Value, StoreError> {
        let jobs = self.jobs_of(run_id).await?;
        let mut map = serde_json::Map::new();
        for job in &jobs {
            let entry = map
                .entry(job.base_id.clone())
                .or_insert_with(|| serde_json::json!({"result": "success", "outputs": {}}));
            let current = entry
                .get("result")
                .and_then(|v| v.as_str())
                .unwrap_or("success")
                .to_string();
            let worst = worse_of(&current, &job.status);
            entry["result"] = serde_json::Value::String(worst);
            if let (Some(dst), Some(src)) =
                (entry["outputs"].as_object_mut(), job.outputs.as_object())
            {
                for (k, v) in src {
                    dst.insert(k.clone(), v.clone());
                }
            }
        }
        Ok(serde_json::Value::Object(map))
    }

    // ---- steps ----------------------------------------------------------

    pub async fn create_step(
        &self,
        step_id: &str,
        job_id: &str,
        idx: i32,
        name: &str,
        uses: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO ci_step (id, job_id, idx, name, uses, status)
             VALUES ($1,$2,$3,$4,$5,'pending')
             ON CONFLICT (job_id, idx) DO NOTHING",
        )
        .bind(step_id)
        .bind(job_id)
        .bind(idx)
        .bind(name)
        .bind(uses)
        .execute(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(())
    }

    pub async fn start_step(&self, step_id: &str, operation_id: &str) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE ci_step
                SET status='running', operation_id=$2, started_at=COALESCE(started_at, now())
              WHERE id = $1",
        )
        .bind(step_id)
        .bind(operation_id)
        .execute(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(())
    }

    pub async fn finish_step(
        &self,
        step_id: &str,
        status: StepStatus,
        exit_code: Option<i32>,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE ci_step
                SET status=$2, exit_code=$3, error=$4, finished_at=now()
              WHERE id = $1",
        )
        .bind(step_id)
        .bind(status.as_str())
        .bind(exit_code)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(())
    }

    pub async fn steps_of(&self, job_id: &str) -> Result<Vec<StepRow>, StoreError> {
        let rows = sqlx::query("SELECT * FROM ci_step WHERE job_id = $1 ORDER BY idx")
            .bind(job_id)
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::sql)?;
        Ok(rows.iter().map(StepRow::from_row).collect())
    }

    // ---- logs -----------------------------------------------------------

    /// Where a step's log lives: `<log_dir>/<run>/<job_key>/<idx>-<step>.log`.
    pub fn log_path(&self, run_id: &str, job_key: &str, idx: i32, step_id: &str) -> PathBuf {
        self.log_dir
            .join(sanitize_component(run_id))
            .join(sanitize_component(job_key))
            .join(format!("{idx:03}-{}.log", sanitize_component(step_id)))
    }

    /// Append to a step's log and record the new size.
    ///
    /// The path is written on the row every time rather than only on the first
    /// append, so a row created before the file existed still points at it.
    pub async fn append_log(
        &self,
        step_id: &str,
        path: &Path,
        text: &str,
    ) -> Result<(), StoreError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StoreError::LogDir {
                    path: parent.to_path_buf(),
                    reason: e.to_string(),
                })?;
        }
        use tokio::io::AsyncWriteExt;
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| StoreError::LogDir {
                path: path.to_path_buf(),
                reason: e.to_string(),
            })?;
        f.write_all(text.as_bytes())
            .await
            .map_err(|e| StoreError::LogDir {
                path: path.to_path_buf(),
                reason: e.to_string(),
            })?;

        sqlx::query("UPDATE ci_step SET log_path = $2, log_bytes = log_bytes + $3 WHERE id = $1")
            .bind(step_id)
            .bind(path.to_string_lossy().as_ref())
            .bind(text.len() as i64)
            .execute(&self.pool)
            .await
            .map_err(StoreError::sql)?;
        Ok(())
    }

    pub async fn read_log(&self, step: &StepRow) -> Option<String> {
        let path = step.log_path.as_ref()?;
        tokio::fs::read_to_string(path).await.ok()
    }

    pub async fn record_artifact(
        &self,
        run_id: &str,
        job_id: &str,
        name: &str,
        stored: &crate::artifacts::StoredArtifact,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO ci_artifact (id, run_id, job_id, name, sink, digest, size_bytes, uri)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(crate::vm::new_id())
        .bind(run_id)
        .bind(job_id)
        .bind(name)
        .bind(stored.sink)
        .bind(&stored.digest)
        .bind(stored.size_bytes as i64)
        .bind(&stored.uri)
        .execute(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(())
    }

    pub async fn artifacts_of(&self, run_id: &str) -> Result<Vec<ArtifactRow>, StoreError> {
        let rows = sqlx::query("SELECT * FROM ci_artifact WHERE run_id = $1 ORDER BY created_at")
            .bind(run_id)
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::sql)?;
        Ok(rows
            .iter()
            .map(|r| ArtifactRow {
                name: r.get("name"),
                sink: r.get("sink"),
                digest: r.get("digest"),
                size_bytes: r.get("size_bytes"),
                uri: r.get("uri"),
            })
            .collect())
    }

    // ---- users ----------------------------------------------------------

    /// Record a person and return their role, seeding admins from config.
    ///
    /// Keyed on the stable subject, so an email change updates the row rather
    /// than creating a second account for the same person.
    pub async fn upsert_user(
        &self,
        subject: &str,
        email: &str,
        name: Option<&str>,
        admin_emails: &[String],
    ) -> Result<String, StoreError> {
        let seed_role = if admin_emails
            .iter()
            .any(|a| a == &email.to_ascii_lowercase())
        {
            "admin"
        } else {
            "viewer"
        };
        let row = sqlx::query(
            "INSERT INTO ci_user (subject, email, name, role)
             VALUES ($1,$2,$3,$4)
             ON CONFLICT (subject) DO UPDATE
                SET email = EXCLUDED.email,
                    name = EXCLUDED.name,
                    last_seen_at = now(),
                    -- Promote on the config list, but never demote: a role
                    -- granted in the UI must survive the list changing.
                    role = CASE WHEN EXCLUDED.role = 'admin' THEN 'admin'
                                ELSE ci_user.role END
             RETURNING role",
        )
        .bind(subject)
        .bind(email)
        .bind(name)
        .bind(seed_role)
        .fetch_one(&self.pool)
        .await
        .map_err(StoreError::sql)?;
        Ok(row.get("role"))
    }
}

/// A job's row id, derived from the run and the job key so it is stable across
/// a redelivery — the same job always resolves to the same row.
pub fn job_id(run_id: &str, job_key: &str) -> String {
    format!("{run_id}.{job_key}")
}

/// A step's row id, likewise derived and therefore reusable as the daemon's
/// `operationId`: re-running the same step of the same job reattaches.
pub fn step_id(job_id: &str, idx: usize) -> String {
    format!("{job_id}.{idx}")
}

/// Rank two job statuses and return the worse, for rolling matrix cells up into
/// one `needs.<base>.result`.
fn worse_of(a: &str, b: &str) -> String {
    fn rank(s: &str) -> u8 {
        match s {
            "success" => 0,
            "skipped" => 1,
            "pending" | "queued" | "running" => 2,
            "cancelled" => 3,
            _ => 4, // failure
        }
    }
    if rank(b) > rank(a) {
        b.to_string()
    } else {
        a.to_string()
    }
}

/// Keep a path component inside the log directory.
///
/// Ids are already charset-restricted, but a log path is assembled from values
/// that reach us over HTTP, and one `..` would write outside `CI_LOG_DIR`.
fn sanitize_component(s: &str) -> String {
    // `.` is folded away too, not just `/`. Keeping dots would be safe — a
    // component with no separator cannot traverse — but it produces names like
    // `..-..-etc-passwd` that read as an attempted escape to anyone auditing
    // the directory, and hidden files that `ls` does not show.
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        let c = if c.is_ascii_alphanumeric() || c == '_' {
            c
        } else {
            '-'
        };
        if c == '-' && out.ends_with('-') {
            continue;
        }
        out.push(c);
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        return "-".to_string();
    }
    trimmed.to_string()
}

/// Steps to create for a job, in order.
pub fn step_rows_for(plan: &JobPlan, job_id: &str) -> Vec<(String, i32, String, Option<String>)> {
    plan.steps
        .iter()
        .enumerate()
        .map(|(i, s)| (step_id(job_id, i), i as i32, s.label(i), s.uses.clone()))
        .collect()
}

#[derive(Debug)]
pub enum StoreError {
    Connect(String),
    Migrations { path: PathBuf, reason: String },
    LogDir { path: PathBuf, reason: String },
    Sql(String),
}

impl StoreError {
    fn sql(e: sqlx::Error) -> Self {
        Self::Sql(e.to_string())
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(e) => write!(
                f,
                "could not connect to Postgres: {e}. Check CI_DATABASE_URL."
            ),
            Self::Migrations { path, reason } => {
                write!(f, "migration {} failed: {reason}", path.display())
            }
            Self::LogDir { path, reason } => {
                write!(f, "could not write logs under {}: {reason}", path.display())
            }
            Self::Sql(e) => write!(f, "database error: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_round_trip_through_their_wire_names() {
        assert_eq!(RunStatus::Success.as_str(), "success");
        assert!(RunStatus::Failure.is_terminal());
        assert!(!RunStatus::Running.is_terminal());
        assert!(JobStatus::Skipped.is_terminal());
        assert!(!JobStatus::Queued.is_terminal());
        assert_eq!(StepStatus::Running.as_str(), "running");
    }

    /// A skipped dependency is not a failure — a downstream `if:` may want to
    /// run anyway, and GitHub reports it as `skipped`.
    #[test]
    fn a_skipped_job_reports_skipped_not_failure() {
        assert_eq!(JobStatus::Skipped.result_name(), "skipped");
        assert_eq!(JobStatus::Failure.result_name(), "failure");
        assert_eq!(JobStatus::Running.result_name(), "pending");
    }

    /// `needs.build.result` must mean "did build pass", so one failed matrix
    /// cell has to poison the whole base id.
    #[test]
    fn rolling_matrix_cells_up_takes_the_worst_result() {
        assert_eq!(worse_of("success", "failure"), "failure");
        assert_eq!(worse_of("failure", "success"), "failure");
        assert_eq!(worse_of("success", "skipped"), "skipped");
        assert_eq!(worse_of("skipped", "success"), "skipped");
        assert_eq!(worse_of("success", "running"), "running");
        assert_eq!(worse_of("cancelled", "failure"), "failure");
        assert_eq!(worse_of("success", "success"), "success");
    }

    /// Ids are derived rather than minted so a redelivery addresses the same
    /// rows, and a step id doubles as the daemon's idempotent `operationId`.
    #[test]
    fn ids_are_derived_and_therefore_stable() {
        let run = "019f7c7ef325-00000000";
        let j = job_id(run, "build-x86_64");
        assert_eq!(j, "019f7c7ef325-00000000.build-x86_64");
        assert_eq!(job_id(run, "build-x86_64"), j, "deriving twice agrees");

        let s = step_id(&j, 2);
        assert_eq!(s, "019f7c7ef325-00000000.build-x86_64.2");
        assert!(
            crate::vm::valid_operation_id(&s),
            "a step id must be usable as an operationId: {s}"
        );
    }

    // ---- integration ----------------------------------------------------
    //
    // Need a Postgres. Run with:
    //   CI_TEST_DATABASE_URL=postgres://user:pass@127.0.0.1:5432/ci_test \
    //     cargo test -- --ignored store::
    //
    // Each test uses its own run id, so they are safe to run against a database
    // that already has rows in it and safe to run concurrently.

    async fn test_store() -> Store {
        let url = std::env::var("CI_TEST_DATABASE_URL").expect("CI_TEST_DATABASE_URL");
        let dir = std::env::temp_dir().join(format!("ci-test-logs-{}", crate::vm::new_id()));
        let store = Store::connect(&url, dir).await.expect("connects");
        store
            .migrate(Path::new("migrations"))
            .await
            .expect("migrations apply");
        store
    }

    fn test_plan() -> Plan {
        let wf = crate::workflow::Workflow::parse(
            "wf.yml",
            r#"
name: test
jobs:
  build:
    vm: { driver: firecracker }
    strategy:
      matrix:
        target: [x86_64, aarch64]
    steps:
      - name: Compile
        run: "true"
      - name: Test
        run: "true"
  deploy:
    needs: [build]
    vm: { driver: firecracker }
    steps: [{ name: Ship, run: "true" }]
"#,
        )
        .expect("workflow parses");
        Plan::build(&wf).expect("plan builds")
    }

    /// Re-running every migration on every startup is the whole scheme, so it
    /// has to actually be idempotent rather than merely intended to be.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn migrations_are_idempotent() {
        let store = test_store().await;
        for _ in 0..3 {
            store
                .migrate(Path::new("migrations"))
                .await
                .expect("re-applying migrations is a no-op");
        }
    }

    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn a_run_and_its_jobs_are_created_together() {
        let store = test_store().await;
        let plan = test_plan();
        let run_id = crate::vm::new_id();

        store
            .create_run(&run_id, &RunRequest::default(), &plan)
            .await
            .expect("run created");

        let run = store.get_run(&run_id).await.unwrap().expect("run exists");
        assert_eq!(run.status, "queued");
        assert_eq!(run.workflow_name.as_deref(), Some("test"));

        let jobs = store.jobs_of(&run_id).await.unwrap();
        assert_eq!(jobs.len(), 3, "two matrix cells plus deploy");
        assert!(jobs.iter().all(|j| j.status == "pending"));
        let keys: Vec<&str> = jobs.iter().map(|j| j.job_key.as_str()).collect();
        assert!(keys.contains(&"build-x86_64"), "{keys:?}");
        assert!(keys.contains(&"deploy"), "{keys:?}");
    }

    /// The guard that makes a JetStream redelivery safe: work that already
    /// finished must not be restarted by a second delivery.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn starting_a_finished_job_is_refused() {
        let store = test_store().await;
        let plan = test_plan();
        let run_id = crate::vm::new_id();
        store
            .create_run(&run_id, &RunRequest::default(), &plan)
            .await
            .unwrap();
        let jid = job_id(&run_id, "deploy");

        assert!(
            store
                .start_job(&jid, "hd-1", "sb-1", "fp", 1)
                .await
                .unwrap(),
            "a pending job starts"
        );
        store
            .set_job_status(&jid, JobStatus::Success, None)
            .await
            .unwrap();
        assert!(
            !store
                .start_job(&jid, "hd-1", "sb-2", "fp", 2)
                .await
                .unwrap(),
            "a finished job must not be restarted by a redelivery"
        );

        let job = store.get_job(&jid).await.unwrap().unwrap();
        assert_eq!(job.status, "success");
        assert_eq!(job.sandbox_id.as_deref(), Some("sb-1"), "not overwritten");
    }

    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn a_run_rolls_up_from_its_jobs() {
        let store = test_store().await;
        let plan = test_plan();
        let run_id = crate::vm::new_id();
        store
            .create_run(&run_id, &RunRequest::default(), &plan)
            .await
            .unwrap();

        // Nothing finished yet.
        assert_eq!(
            store.roll_up_run(&run_id).await.unwrap(),
            RunStatus::Running
        );

        for key in ["build-x86_64", "build-aarch64", "deploy"] {
            store
                .set_job_status(&job_id(&run_id, key), JobStatus::Success, None)
                .await
                .unwrap();
        }
        assert_eq!(
            store.roll_up_run(&run_id).await.unwrap(),
            RunStatus::Success
        );
        let run = store.get_run(&run_id).await.unwrap().unwrap();
        assert!(
            run.finished_at.is_some(),
            "a terminal run gets a finish time"
        );

        // One failure poisons the run even though the rest passed.
        store
            .set_job_status(&job_id(&run_id, "deploy"), JobStatus::Failure, Some("boom"))
            .await
            .unwrap();
        assert_eq!(
            store.roll_up_run(&run_id).await.unwrap(),
            RunStatus::Failure
        );
    }

    /// A skipped job must not make the run fail — that is how a conditional
    /// deploy job behaves on a branch that does not deploy.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn a_skipped_job_still_lets_a_run_succeed() {
        let store = test_store().await;
        let plan = test_plan();
        let run_id = crate::vm::new_id();
        store
            .create_run(&run_id, &RunRequest::default(), &plan)
            .await
            .unwrap();
        for key in ["build-x86_64", "build-aarch64"] {
            store
                .set_job_status(&job_id(&run_id, key), JobStatus::Success, None)
                .await
                .unwrap();
        }
        store
            .set_job_status(&job_id(&run_id, "deploy"), JobStatus::Skipped, None)
            .await
            .unwrap();
        assert_eq!(
            store.roll_up_run(&run_id).await.unwrap(),
            RunStatus::Success
        );
    }

    /// `needs.build.result` means "did build pass". One failed cell has to make
    /// it `failure`, or a deploy job runs off a broken build.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn needs_context_collapses_matrix_cells_to_the_worst_result() {
        let store = test_store().await;
        let plan = test_plan();
        let run_id = crate::vm::new_id();
        store
            .create_run(&run_id, &RunRequest::default(), &plan)
            .await
            .unwrap();

        store
            .set_job_status(&job_id(&run_id, "build-x86_64"), JobStatus::Success, None)
            .await
            .unwrap();
        store
            .set_job_outputs(
                &job_id(&run_id, "build-x86_64"),
                &serde_json::json!({"sha": "abc123"}),
            )
            .await
            .unwrap();
        store
            .set_job_status(&job_id(&run_id, "build-aarch64"), JobStatus::Failure, None)
            .await
            .unwrap();

        let needs = store.needs_context(&run_id).await.unwrap();
        assert_eq!(needs["build"]["result"], "failure", "{needs}");
        assert_eq!(needs["build"]["outputs"]["sha"], "abc123");

        // And that context makes the guard on a dependent job evaluate false.
        let mut ctx = crate::expr::Context::new();
        ctx.set("needs", needs);
        assert!(
            !ctx.eval_condition("needs.build.result == 'success'")
                .unwrap()
        );
    }

    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn steps_record_their_outcome_and_their_logs_land_on_disk() {
        let store = test_store().await;
        let plan = test_plan();
        let run_id = crate::vm::new_id();
        store
            .create_run(&run_id, &RunRequest::default(), &plan)
            .await
            .unwrap();

        let jid = job_id(&run_id, "build-x86_64");
        let cell = plan.jobs.iter().find(|j| j.key == "build-x86_64").unwrap();
        for (sid, idx, name, uses) in step_rows_for(cell, &jid) {
            store
                .create_step(&sid, &jid, idx, &name, uses.as_deref())
                .await
                .unwrap();
        }

        let steps = store.steps_of(&jid).await.unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].name, "Compile");
        assert_eq!(steps[1].name, "Test");

        let sid = &steps[0].id;
        store.start_step(sid, sid).await.unwrap();
        let path = store.log_path(&run_id, "build-x86_64", 0, sid);
        store.append_log(sid, &path, "compiling\n").await.unwrap();
        store.append_log(sid, &path, "done\n").await.unwrap();
        store
            .finish_step(sid, StepStatus::Success, Some(0), None)
            .await
            .unwrap();

        let steps = store.steps_of(&jid).await.unwrap();
        assert_eq!(steps[0].status, "success");
        assert_eq!(steps[0].exit_code, Some(0));
        assert_eq!(steps[0].log_bytes, 15, "both appends counted");
        assert_eq!(
            store.read_log(&steps[0]).await.as_deref(),
            Some("compiling\ndone\n")
        );

        tokio::fs::remove_dir_all(path.parent().unwrap().parent().unwrap())
            .await
            .ok();
    }

    /// Creating steps must be safe to repeat — a redelivered job re-creates its
    /// step rows before running anything.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn creating_the_same_step_twice_is_a_no_op() {
        let store = test_store().await;
        let plan = test_plan();
        let run_id = crate::vm::new_id();
        store
            .create_run(&run_id, &RunRequest::default(), &plan)
            .await
            .unwrap();
        let jid = job_id(&run_id, "deploy");
        for _ in 0..3 {
            store
                .create_step(&step_id(&jid, 0), &jid, 0, "Ship", None)
                .await
                .expect("repeatable");
        }
        assert_eq!(store.steps_of(&jid).await.unwrap().len(), 1);
    }

    /// Being on the admin list promotes, but dropping off it must not demote —
    /// a role granted in the UI has to survive the env var changing.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn admin_seeding_promotes_but_never_demotes() {
        let store = test_store().await;
        let subject = format!("sub-{}", crate::vm::new_id());
        let admins = vec!["boss@example.com".to_string()];

        let role = store
            .upsert_user(&subject, "nobody@example.com", None, &admins)
            .await
            .unwrap();
        assert_eq!(role, "viewer");

        let role = store
            .upsert_user(&subject, "boss@example.com", Some("Boss"), &admins)
            .await
            .unwrap();
        assert_eq!(role, "admin", "the config list promotes");

        let role = store
            .upsert_user(&subject, "boss@example.com", Some("Boss"), &[])
            .await
            .unwrap();
        assert_eq!(role, "admin", "an empty list must not demote");
    }

    /// A log path is assembled from values that arrive over HTTP; one `..`
    /// would write outside CI_LOG_DIR.
    #[test]
    fn log_path_components_cannot_escape_the_log_directory() {
        assert_eq!(sanitize_component("../../etc/passwd"), "etc-passwd");
        assert_eq!(sanitize_component(".."), "-");
        assert_eq!(sanitize_component("."), "-");
        assert_eq!(sanitize_component(""), "-");
        assert_eq!(sanitize_component("/"), "-");
        // Ids and job keys survive intact — they are already in the alphabet.
        assert_eq!(sanitize_component("build-x86_64"), "build-x86_64");
        assert_eq!(
            sanitize_component("019f7c7ef325-00000000"),
            "019f7c7ef325-00000000"
        );

        let store_dir = PathBuf::from("/var/lib/ci-logs");
        let joined = store_dir
            .join(sanitize_component("../.."))
            .join(sanitize_component("x"));
        assert!(
            joined.starts_with("/var/lib/ci-logs"),
            "escaped: {}",
            joined.display()
        );
    }
}
