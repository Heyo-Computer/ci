//! Configuration, resolved from `CI_*` environment variables.
//!
//! There are no CLI arguments — the house convention (app-lb, queue-fn, app-obs)
//! is environment-only config, because the process is started by supervisord and
//! a flag that only exists in one unit file is a flag nobody finds.
//!
//! **A misconfiguration is a startup failure, not a degraded service.** Every
//! value is resolved and validated in [`Config::from_env`] before anything binds
//! a port or dials NATS, and every [`ConfigError`] names the variable to fix.
//! The alternative — discovering at the first job that `CI_NETWORK` was never
//! set — costs a run and reads as a heyvm outage.
//!
//! One thing deliberately *not* here: the NATS credential. It lives in
//! [`crate::nats_auth::NatsEndpoint`], which has no `Debug` that reveals it, so a
//! `{:?}` on `Config` cannot print a password.

use crate::nats_auth::{EnvCredentials, NatsAuthError, NatsEndpoint};
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Where build artifacts go. Chosen once at startup; a workflow object may
/// override it per workflow later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactSinkKind {
    /// A directory on the orchestrator host.
    Disk,
    /// An S3 bucket.
    S3,
    /// The `artifacts` content-addressed store, over HTTP.
    Artifacts,
}

impl ArtifactSinkKind {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disk" | "file" | "local" => Some(Self::Disk),
            "s3" => Some(Self::S3),
            "artifacts" | "art" => Some(Self::Artifacts),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disk => "disk",
            Self::S3 => "s3",
            Self::Artifacts => "artifacts",
        }
    }
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ArtifactsConfig {
    /// Base URL of one central `art serve`.
    ///
    /// One, not per-host: the store is per-host and per-user (its own
    /// `examples/artifacts.json` pins it to a single replica, because "each VM
    /// has its own disk, so N replicas are N independent stores"). The
    /// orchestrator pulls each artifact out of the guest and pushes it here, so
    /// runners never talk to the store and there is nothing to sync.
    pub url: String,
    pub token: Option<String>,
}

/// The heyvm control-plane connection and the network whose hosts are runners.
#[derive(Debug, Clone)]
pub struct HeyvmConfig {
    /// Cloud base URL. `None` uses the SDK default (`https://server.heyo.computer`).
    pub base_url: Option<String>,
    /// Bearer for the cloud *and* for each runner daemon.
    pub api_key: String,
    /// Network name or id whose `host` members are the runner pool.
    pub network: String,
    /// Optional iroh relay override for NAT traversal.
    pub relay: Option<String>,
    /// How often the runner set is re-read from the control plane.
    pub refresh_interval: Duration,
    /// How long a job pinned to an offline runner waits before failing.
    pub runner_wait: Duration,
    /// Backstop TTL on every VM we create, so a crashed orchestrator does not
    /// strand a fleet. Renewed while a VM is pooled.
    pub vm_ttl: Duration,
    /// Drive one local `heyvmd` directly instead of discovering hosts through
    /// the cloud.
    ///
    /// `CI_LOCAL_RUNNER=1` uses `http://127.0.0.1:34099`; any other value is
    /// taken as the daemon's base URL. The pool becomes a single runner named
    /// `local`, and no iroh tunnel or cloud account is involved.
    ///
    /// This exists because the development loop otherwise needs a registered
    /// daemon *and* a network membership before a single job can run — and
    /// because it is the only configuration a test can assert against without a
    /// second machine. queue-fn is local-daemon-only for the same reason.
    pub local_runner: Option<String>,
}

/// Default for `CI_ALLOW_UNAUTHENTICATED_RUNNERS`.
///
/// Refusing is the default because an iroh ticket is bearer-equivalent — a
/// daemon with no `JWT_SECRET` hands a host shell to anyone who has seen its
/// ticket. Opting out is for a single-machine loop where the tunnel never
/// leaves localhost.
const ALLOW_UNAUTHENTICATED_RUNNERS: bool = false;

#[derive(Debug)]
pub struct Config {
    pub name: String,
    pub listen_addr: SocketAddr,
    /// Absolute base URL this app is reachable at, with no trailing slash. Used
    /// for links in the dashboard and for exec-operation callbacks.
    pub public_url: String,

    pub heyvm: HeyvmConfig,

    pub nats: NatsEndpoint,
    /// Subject and stream namespace. Interpolated into subjects unescaped, so it
    /// is charset-validated here.
    pub nats_prefix: String,

    /// Postgres connection string. Not dialed until the store is built.
    pub database_url: String,
    /// Where step logs are appended. Postgres holds the path and byte length;
    /// multi-megabyte log blobs in a row would be a mistake.
    pub log_dir: PathBuf,
    /// Where a run's checkout is materialized. Read by the fingerprint to hash
    /// `cache_key_files`, and by nothing else — the guest gets the repository
    /// over git, not from here.
    pub workspace_dir: PathBuf,
    /// Where `*.sql` migrations live. Overridable because the process runs from
    /// a deploy directory that is not always the source tree.
    pub migrations_dir: PathBuf,

    pub artifact_sink: ArtifactSinkKind,
    pub artifact_dir: PathBuf,
    pub s3: Option<S3Config>,
    pub artifacts: Option<ArtifactsConfig>,

    /// heyosecret base URL and its one all-powerful bearer. This process is the
    /// policy layer — heyosecret has no per-namespace authorization, so the
    /// token must never reach a build.
    pub heyosecret_url: Option<String>,
    pub heyosecret_token: Option<String>,

    /// app-lb admin API, where `workflow` objects live.
    pub app_lb_url: Option<String>,
    pub app_lb_token: Option<String>,

    /// Shared secret the `git ci` client HMACs its payload with.
    pub webhook_secret: String,
    /// Glob for workflow files inside a submitted tree.
    ///
    /// A workflow object will override this per repository; until then it is the
    /// one place the convention lives.
    pub default_workflow_path: String,
    /// Ceiling on a submitted source archive, after base64 decoding.
    ///
    /// The whole tree arrives in one JSON body and is then written into a guest
    /// the same way, so this bounds both the request and the upload.
    pub max_source_bytes: usize,

    /// Emails seeded as admins on first sight. app-lb has no roles, so this app
    /// keeps its own.
    pub admin_emails: Vec<String>,

    /// Whether to accept a runner whose daemon serves its API with no auth. See
    /// [`ALLOW_UNAUTHENTICATED_RUNNERS`].
    pub allow_unauthenticated_runners: bool,

    /// Ceiling on any one job, and the basis for JetStream's `ack_wait`.
    ///
    /// A consumer is bound before it knows which job it will get, so `ack_wait`
    /// has to be derived from the longest job that is allowed to exist. Set it
    /// below a real job's runtime and JetStream redelivers healthy work
    /// mid-build, putting two dispatchers on one VM.
    pub max_job_duration: Duration,

    /// Signs short-TTL, run-scoped tokens for the SSE log stream.
    ///
    /// Random per process, like the artifacts dashboard's session token. A
    /// restart invalidating open log streams is correct — the browser
    /// reconnects and the page it reconnects from was itself re-fetched through
    /// app-lb's gate.
    pub stream_key: [u8; 32],
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let name = opt("CI_NAME").unwrap_or_else(|| "ci".to_string());

        let listen_raw = opt("CI_LISTEN_ADDR").unwrap_or_else(|| "127.0.0.1:9500".to_string());
        let listen_addr = listen_raw
            .parse::<SocketAddr>()
            .map_err(|e| ConfigError::BadValue {
                var: "CI_LISTEN_ADDR",
                value: listen_raw.clone(),
                reason: e.to_string(),
            })?;

        let public_url = opt("CI_PUBLIC_URL")
            .unwrap_or_else(|| format!("http://{listen_addr}"))
            .trim_end_matches('/')
            .to_string();

        // `HEYO_API_KEY` is the SDK's own fallback and what `heyvm login` writes
        // into the environment, so accept it rather than forcing a second name.
        let api_key = opt("CI_HEYO_API_KEY")
            .or_else(|| opt("HEYO_API_KEY"))
            .ok_or(ConfigError::Missing {
                var: "CI_HEYO_API_KEY",
                purpose: "the bearer for the heyvm cloud and for each runner daemon \
                          (HEYO_API_KEY is also accepted)",
            })?;

        let network = opt("CI_NETWORK").ok_or(ConfigError::Missing {
            var: "CI_NETWORK",
            purpose: "the heyvm network whose `host` members are the runner pool",
        })?;

        let heyvm = HeyvmConfig {
            base_url: opt("CI_HEYO_BASE_URL"),
            api_key,
            network,
            relay: opt("CI_IROH_RELAY"),
            refresh_interval: secs("CI_RUNNER_REFRESH_SECS", 30)?,
            runner_wait: secs("CI_RUNNER_WAIT_SECS", 900)?,
            vm_ttl: secs("CI_VM_TTL_SECONDS", 3600)?,
            local_runner: opt("CI_LOCAL_RUNNER").map(|v| match v.as_str() {
                "1" | "true" | "yes" | "on" => heyo_sdk::DEFAULT_LOCAL_BASE_URL.to_string(),
                other => other.trim_end_matches('/').to_string(),
            }),
        };

        let nats_url = opt("CI_NATS_URL").unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
        let nats = NatsEndpoint::resolve(
            &nats_url,
            &EnvCredentials {
                user: opt("CI_NATS_USER"),
                password: opt("CI_NATS_PASSWORD"),
                token: opt("CI_NATS_TOKEN"),
                creds_file: opt("CI_NATS_CREDS"),
                nkey_seed: opt("CI_NATS_NKEY"),
            },
        )?;

        let nats_prefix = opt("CI_NATS_SUBJECT_PREFIX").unwrap_or_else(|| "ci".to_string());
        if !is_subject_token(&nats_prefix) {
            return Err(ConfigError::BadValue {
                var: "CI_NATS_SUBJECT_PREFIX",
                value: nats_prefix,
                // Interpolated into subjects and durable consumer names without
                // escaping, exactly as queue-fn does with function ids.
                reason: "must be one or more of [A-Za-z0-9_-]; it becomes a NATS \
                         subject token and a durable consumer name verbatim"
                    .to_string(),
            });
        }

        let database_url = opt("CI_DATABASE_URL").ok_or(ConfigError::Missing {
            var: "CI_DATABASE_URL",
            purpose: "the Postgres connection string holding runs, jobs, steps and the VM pool",
        })?;

        let log_dir = PathBuf::from(opt("CI_LOG_DIR").unwrap_or_else(|| "ci-logs".to_string()));
        let workspace_dir =
            PathBuf::from(opt("CI_WORKSPACE_DIR").unwrap_or_else(|| "ci-workspaces".to_string()));

        let sink_raw = opt("CI_ARTIFACT_SINK").unwrap_or_else(|| "disk".to_string());
        let artifact_sink = ArtifactSinkKind::parse(&sink_raw).ok_or(ConfigError::BadValue {
            var: "CI_ARTIFACT_SINK",
            value: sink_raw.clone(),
            reason: "must be one of: disk, s3, artifacts".to_string(),
        })?;
        let artifact_dir =
            PathBuf::from(opt("CI_ARTIFACT_DIR").unwrap_or_else(|| "ci-artifacts".to_string()));

        // Companion variables are required only for the sink actually selected —
        // but when it *is* selected, missing ones fail here rather than at the
        // first upload, which would be after a green build.
        let s3 = match artifact_sink {
            ArtifactSinkKind::S3 => Some(S3Config {
                bucket: opt("CI_S3_BUCKET").ok_or(ConfigError::Missing {
                    var: "CI_S3_BUCKET",
                    purpose: "the bucket artifacts go to, required by CI_ARTIFACT_SINK=s3",
                })?,
                prefix: opt("CI_S3_PREFIX").unwrap_or_else(|| "ci".to_string()),
                region: opt("CI_S3_REGION"),
                endpoint: opt("CI_S3_ENDPOINT"),
            }),
            _ => None,
        };
        let artifacts = match artifact_sink {
            ArtifactSinkKind::Artifacts => Some(ArtifactsConfig {
                url: opt("CI_ARTIFACT_URL")
                    .ok_or(ConfigError::Missing {
                        var: "CI_ARTIFACT_URL",
                        purpose: "the base URL of one central `art serve`, required by \
                                  CI_ARTIFACT_SINK=artifacts",
                    })?
                    .trim_end_matches('/')
                    .to_string(),
                token: opt("CI_ARTIFACT_TOKEN"),
            }),
            _ => None,
        };

        let webhook_secret = opt("CI_WEBHOOK_SECRET").ok_or(ConfigError::Missing {
            var: "CI_WEBHOOK_SECRET",
            purpose: "the HMAC-SHA256 secret `git ci` signs its payload with",
        })?;
        if webhook_secret.len() < 16 {
            return Err(ConfigError::BadValue {
                var: "CI_WEBHOOK_SECRET",
                // The value is a secret, so the error names its length, not itself.
                value: format!("<{} bytes>", webhook_secret.len()),
                reason: "must be at least 16 bytes; this is the only thing standing \
                         between the public submit endpoint and arbitrary code \
                         execution on a runner"
                    .to_string(),
            });
        }

        let admin_emails = opt("CI_ADMIN_EMAILS")
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            name,
            listen_addr,
            public_url,
            heyvm,
            nats,
            nats_prefix,
            database_url,
            log_dir,
            workspace_dir,
            migrations_dir: PathBuf::from(
                opt("CI_MIGRATIONS_DIR").unwrap_or_else(|| "migrations".to_string()),
            ),
            artifact_sink,
            artifact_dir,
            s3,
            artifacts,
            heyosecret_url: opt("CI_HEYOSECRET_URL")
                .or_else(|| opt("HEYOSECRET_URL"))
                .map(|u| u.trim_end_matches('/').to_string()),
            heyosecret_token: opt("CI_HEYOSECRET_TOKEN")
                .or_else(|| opt("HEYOSECRET_INTERNAL_API_KEY"))
                .or_else(|| opt("PLATFORM_INTERNAL_API_KEY")),
            app_lb_url: opt("CI_APP_LB_URL").map(|u| u.trim_end_matches('/').to_string()),
            app_lb_token: opt("CI_APP_LB_TOKEN"),
            webhook_secret,
            default_workflow_path: opt("CI_WORKFLOW_PATH")
                .unwrap_or_else(|| ".ci/workflows/*.yml".to_string()),
            max_source_bytes: bytes("CI_MAX_SOURCE_BYTES", 64 * 1024 * 1024)?,
            admin_emails,
            max_job_duration: secs("CI_MAX_JOB_SECONDS", 4 * 60 * 60)?,
            allow_unauthenticated_runners: flag(
                "CI_ALLOW_UNAUTHENTICATED_RUNNERS",
                ALLOW_UNAUTHENTICATED_RUNNERS,
            )?,
            stream_key: random_key(),
        })
    }

    /// One line naming every resolved setting that is safe to print. Logged at
    /// startup so a misbehaving deployment can be diagnosed from its own output
    /// rather than from someone's memory of the unit file.
    pub fn summary(&self) -> String {
        format!(
            "listen={} public={} network={} nats={} prefix={} sink={} logs={} \
             heyosecret={} app-lb={} admins={}",
            self.listen_addr,
            self.public_url,
            self.heyvm
                .local_runner
                .as_deref()
                .map(|u| format!("local:{u}"))
                .unwrap_or_else(|| self.heyvm.network.clone()),
            self.nats.redacted(),
            self.nats_prefix,
            self.artifact_sink.as_str(),
            self.log_dir.display(),
            self.heyosecret_url.as_deref().unwrap_or("unset"),
            self.app_lb_url.as_deref().unwrap_or("unset"),
            self.admin_emails.len(),
        )
    }
}

fn opt(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => None,
    }
}

/// A boolean env var.
///
/// Unparseable values are an error rather than falsey. `CI_ALLOW_UNAUTHENTICATED_RUNNERS=yes`
/// silently meaning `false` would be a security setting that reads as applied
/// and is not.
fn flag(var: &'static str, default: bool) -> Result<bool, ConfigError> {
    match opt(var) {
        None => Ok(default),
        Some(raw) => match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(ConfigError::BadValue {
                var,
                value: raw,
                reason: "must be one of: true, false, 1, 0, yes, no, on, off".to_string(),
            }),
        },
    }
}

fn bytes(var: &'static str, default: usize) -> Result<usize, ConfigError> {
    match opt(var) {
        None => Ok(default),
        Some(raw) => {
            let n = raw.parse::<usize>().map_err(|e| ConfigError::BadValue {
                var,
                value: raw.clone(),
                reason: e.to_string(),
            })?;
            if n == 0 {
                return Err(ConfigError::BadValue {
                    var,
                    value: raw,
                    reason: "must be greater than zero".to_string(),
                });
            }
            Ok(n)
        }
    }
}

fn secs(var: &'static str, default: u64) -> Result<Duration, ConfigError> {
    match opt(var) {
        None => Ok(Duration::from_secs(default)),
        Some(raw) => {
            let n = raw.parse::<u64>().map_err(|e| ConfigError::BadValue {
                var,
                value: raw.clone(),
                reason: e.to_string(),
            })?;
            if n == 0 {
                return Err(ConfigError::BadValue {
                    var,
                    value: raw,
                    reason: "must be greater than zero".to_string(),
                });
            }
            Ok(Duration::from_secs(n))
        }
    }
}

/// Whether a string may be interpolated into a NATS subject and a durable
/// consumer name without escaping.
pub fn is_subject_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// 32 random bytes, from two v4 UUIDs.
///
/// `uuid` is already a dependency and its v4 generator is a CSPRNG, so this
/// avoids linking `rand` for one call. 122 bits of entropy each, 244 total —
/// well past what signing a 60-second stream token needs.
fn random_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    key
}

#[derive(Debug)]
pub enum ConfigError {
    Missing {
        var: &'static str,
        purpose: &'static str,
    },
    BadValue {
        var: &'static str,
        value: String,
        reason: String,
    },
    Nats(NatsAuthError),
}

impl From<NatsAuthError> for ConfigError {
    fn from(e: NatsAuthError) -> Self {
        Self::Nats(e)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { var, purpose } => {
                write!(f, "{var} is not set. It is {purpose}.")
            }
            Self::BadValue { var, value, reason } => {
                write!(f, "{var}={value:?} is not usable: {reason}")
            }
            Self::Nats(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_sink_kinds_parse_with_their_aliases() {
        assert_eq!(
            ArtifactSinkKind::parse("disk"),
            Some(ArtifactSinkKind::Disk)
        );
        assert_eq!(
            ArtifactSinkKind::parse("LOCAL"),
            Some(ArtifactSinkKind::Disk)
        );
        assert_eq!(ArtifactSinkKind::parse(" s3 "), Some(ArtifactSinkKind::S3));
        assert_eq!(
            ArtifactSinkKind::parse("art"),
            Some(ArtifactSinkKind::Artifacts)
        );
        assert_eq!(ArtifactSinkKind::parse("gcs"), None);
    }

    /// The prefix reaches a subject and a durable consumer name verbatim, so a
    /// dot or a wildcard in it would silently reshape the subject space.
    #[test]
    fn subject_tokens_reject_anything_that_would_reshape_a_subject() {
        assert!(is_subject_token("ci"));
        assert!(is_subject_token("ci-prod_2"));
        assert!(!is_subject_token(""));
        assert!(!is_subject_token("ci.prod"));
        assert!(!is_subject_token("ci.>"));
        assert!(!is_subject_token("ci *"));
    }

    #[test]
    fn a_zero_duration_is_rejected_rather_than_becoming_a_busy_loop() {
        unsafe { std::env::set_var("CI_TEST_ZERO_SECS", "0") };
        let err = secs("CI_TEST_ZERO_SECS", 30).unwrap_err();
        assert!(err.to_string().contains("greater than zero"), "{err}");
        unsafe { std::env::remove_var("CI_TEST_ZERO_SECS") };
    }

    /// Every error must name the variable to fix — that is the whole reason
    /// config is validated up front instead of at first use.
    #[test]
    fn errors_name_the_variable_to_fix() {
        let e = ConfigError::Missing {
            var: "CI_NETWORK",
            purpose: "the heyvm network",
        };
        assert!(e.to_string().starts_with("CI_NETWORK is not set"));

        let e = ConfigError::BadValue {
            var: "CI_LISTEN_ADDR",
            value: "nope".into(),
            reason: "invalid socket address".into(),
        };
        assert!(e.to_string().contains("CI_LISTEN_ADDR"));
        assert!(e.to_string().contains("invalid socket address"));
    }

    /// Regression guard for the one error that must not echo its value: the
    /// webhook secret's length is diagnostic enough.
    #[test]
    fn a_short_webhook_secret_is_reported_by_length_not_by_value() {
        let e = ConfigError::BadValue {
            var: "CI_WEBHOOK_SECRET",
            value: format!("<{} bytes>", "hunter2".len()),
            reason: "must be at least 16 bytes".into(),
        };
        assert!(!e.to_string().contains("hunter2"));
        assert!(e.to_string().contains("<7 bytes>"));
    }

    #[test]
    fn the_stream_key_is_not_all_zeroes() {
        let a = random_key();
        let b = random_key();
        assert_ne!(a, [0u8; 32]);
        assert_ne!(a, b, "two calls must not return the same key");
    }
}
