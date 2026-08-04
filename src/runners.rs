//! The runner pool: heyvm network members that jobs can run on.
//!
//! **A runner is a `heyvmd` host that joined the configured network.** There is
//! no agent to install and nothing to register with this app. Two control-plane
//! reads compose into the pool:
//!
//! - `GET /networks/{id}/members` — members whose `sandbox_kind` is `host`. The
//!   SDK's own doc for [`NetworkMemberKind::Host`] says it: *"A daemon host
//!   machine (`sandbox_ref` = daemon `hd-*` id). Assigning a host to a network
//!   unlocks host-shell access to it."* Membership is the authorization.
//! - `GET /me/daemons` — liveness and the human-readable name. `Online` means a
//!   heartbeat within ~3 minutes.
//!
//! Neither alone is enough. Membership without a daemon row is a host that was
//! unregistered but never removed from the network; a daemon without membership
//! is a machine the operator has not opted into CI. Both are reported rather
//! than silently dropped, because "my runner isn't picking up jobs" is otherwise
//! unanswerable from the dashboard.
//!
//! ## Reaching one
//!
//! The cloud proxies shell and status routes to a daemon but **not sandbox
//! creation**, so driving a runner means dialing it directly:
//! `GET /me/daemons/{id}/connection-ticket` yields an iroh ticket,
//! [`HeyoClient::connect_p2p`] turns it into a client whose `base_url` is a local
//! forwarded port, and every subsequent SDK call rides that link.
//!
//! The tunnel lives in a background task that [`Drop`] aborts, so the cache here
//! is what keeps a runner reachable — dropping the last clone of a client closes
//! its tunnel.
//!
//! ## An iroh ticket is bearer-equivalent
//!
//! `mvm-ctrl/docs/cross-machine-hardening.md` is explicit: the `hey-proxy/tcp/0`
//! ALPN accepts any peer that knows the ticket, and the daemon has no way to
//! verify the peer. If a runner daemon runs without `JWT_SECRET`, its entire
//! HTTP API — including host shell — is open to anyone holding a ticket that may
//! have transited a log. So every tunnel is probed once, unauthenticated, and a
//! daemon that answers is refused by default.

use crate::config::Config;
use arc_swap::ArcSwap;
use heyo_sdk::{
    DaemonInfo, DaemonStatus, Daemons, HeyoClient, HeyoClientOptions, Network, NetworkInfo,
    RequestOptions,
};
use reqwest::Method;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Membership kind for a daemon host, as the control plane spells it.
const MEMBER_KIND_HOST: &str = "host";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerStatus {
    /// Heartbeat within the cloud's ~3 minute window.
    Online,
    /// Registered and a member, but missing heartbeats.
    Stale,
    /// The cloud has flipped the row away from available.
    Offline,
    /// A network member of kind `host` with no matching daemon row — the host
    /// was unregistered but left in the network.
    Orphaned,
}

impl RunnerStatus {
    /// Whether a job may be dispatched here now.
    pub fn is_dispatchable(&self) -> bool {
        matches!(self, Self::Online)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Stale => "stale",
            Self::Offline => "offline",
            Self::Orphaned => "orphaned",
        }
    }
}

impl From<DaemonStatus> for RunnerStatus {
    fn from(s: DaemonStatus) -> Self {
        match s {
            DaemonStatus::Online => Self::Online,
            DaemonStatus::Stale => Self::Stale,
            DaemonStatus::Offline => Self::Offline,
        }
    }
}

/// One host in the pool.
#[derive(Debug, Clone)]
pub struct Runner {
    /// The daemon id (`hd-…`). Stable, and what a job's subject is keyed on.
    pub id: String,
    /// Display name: the daemon's own label, else the network member's device
    /// name, else the id. A workflow's `uses:` may name any of the three.
    pub name: String,
    pub status: RunnerStatus,
    pub last_seen_at: Option<String>,
}

impl Runner {
    /// Whether `needle` addresses this runner — by id or by name,
    /// case-insensitively on the name.
    ///
    /// Ids are matched exactly: an `hd-…` is machine-generated and a
    /// case-folded match on one would only ever hide a typo.
    pub fn matches(&self, needle: &str) -> bool {
        self.id == needle || self.name.eq_ignore_ascii_case(needle)
    }
}

/// An immutable snapshot of the pool, swapped in whole by the refresh loop so
/// readers never take a lock. The same copy-on-write shape as app-lb's registry.
#[derive(Debug, Default)]
pub struct RunnerSet {
    pub network_id: String,
    pub network_name: String,
    pub runners: Vec<Runner>,
    /// Daemons the caller owns that are *not* members of the configured
    /// network. Not usable, but shown on the dashboard so "why isn't my machine
    /// listed" has an answer that names the fix.
    pub unjoined: Vec<Runner>,
    /// `None` when the last refresh succeeded; otherwise why it did not, so a
    /// stale snapshot is visibly stale rather than quietly wrong.
    pub last_error: Option<String>,
}

impl RunnerSet {
    pub fn find(&self, needle: &str) -> Option<&Runner> {
        self.runners.iter().find(|r| r.matches(needle))
    }

    /// Every runner a job may be dispatched to right now.
    pub fn dispatchable(&self) -> impl Iterator<Item = &Runner> {
        self.runners.iter().filter(|r| r.status.is_dispatchable())
    }
}

/// Pool discovery plus the per-runner tunnel cache.
pub struct Runners {
    config: Arc<Config>,
    snapshot: ArcSwap<RunnerSet>,
    /// One client per runner, each owning its iroh tunnel. Keyed on daemon id.
    ///
    /// A `Mutex` rather than a lock-free map because establishing a tunnel is a
    /// network round trip that must not be raced: two concurrent misses for the
    /// same runner would open two tunnels and leak one.
    tunnels: Mutex<HashMap<String, HeyoClient>>,
}

impl Runners {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            snapshot: ArcSwap::from_pointee(RunnerSet::default()),
            tunnels: Mutex::new(HashMap::new()),
        }
    }

    pub fn snapshot(&self) -> Arc<RunnerSet> {
        self.snapshot.load_full()
    }

    fn client_options(&self) -> HeyoClientOptions {
        HeyoClientOptions {
            api_key: Some(self.config.heyvm.api_key.clone()),
            base_url: self.config.heyvm.base_url.clone(),
            timeout: None,
        }
    }

    /// Re-read the pool from the control plane and swap in a new snapshot.
    ///
    /// A failure keeps the previous snapshot and records the reason on it. The
    /// alternative — emptying the pool on a transient cloud error — would fail
    /// every queued job for the duration of a blip.
    pub async fn refresh(&self) -> Result<(), RunnerError> {
        match self.load().await {
            Ok(set) => {
                self.snapshot.store(Arc::new(set));
                Ok(())
            }
            Err(e) => {
                let prev = self.snapshot.load();
                self.snapshot.store(Arc::new(RunnerSet {
                    network_id: prev.network_id.clone(),
                    network_name: prev.network_name.clone(),
                    runners: prev.runners.clone(),
                    unjoined: prev.unjoined.clone(),
                    last_error: Some(e.to_string()),
                }));
                Err(e)
            }
        }
    }

    /// The single synthetic runner backing `CI_LOCAL_RUNNER`.
    ///
    /// Named `local` and given the id `hd-local`, which is a valid subject token
    /// and so routes like any other host.
    fn local_set(url: &str) -> RunnerSet {
        RunnerSet {
            network_id: "local".to_string(),
            network_name: format!("local ({url})"),
            runners: vec![Runner {
                id: "hd-local".to_string(),
                name: "local".to_string(),
                status: RunnerStatus::Online,
                last_seen_at: None,
            }],
            unjoined: Vec::new(),
            last_error: None,
        }
    }

    async fn load(&self) -> Result<RunnerSet, RunnerError> {
        // Local mode short-circuits every cloud call: no account, no network,
        // no tunnel.
        if let Some(url) = &self.config.heyvm.local_runner {
            return Ok(Self::local_set(url));
        }
        let opts = self.client_options();
        let info = self.resolve_network(&opts).await?;

        let network = Network::get(&info.id, self.client_options())
            .await
            .map_err(|e| RunnerError::ControlPlane(format!("GET /networks/{}: {e}", info.id)))?;
        let members = network
            .list_members()
            .await
            .map_err(|e| RunnerError::ControlPlane(format!("list members: {e}")))?;
        let daemons = Daemons::list(self.client_options())
            .await
            .map_err(|e| RunnerError::ControlPlane(format!("GET /me/daemons: {e}")))?;

        let (runners, unjoined) = Self::project(&members, &daemons);
        Ok(RunnerSet {
            network_id: info.id,
            network_name: info.name,
            runners,
            unjoined,
            last_error: None,
        })
    }

    /// Join the two control-plane reads into the pool and the leftovers.
    ///
    /// Pure, so the filtering that decides what counts as a runner is testable
    /// without a control plane. That filtering is the part most likely to be
    /// wrong in a way nobody notices: a real network is mostly `deployed`
    /// members (every sandbox joins one), and treating those as hosts would
    /// route jobs at VMs.
    fn project(
        members: &[heyo_sdk::NetworkMember],
        daemons: &[DaemonInfo],
    ) -> (Vec<Runner>, Vec<Runner>) {
        let by_id: HashMap<&str, &DaemonInfo> =
            daemons.iter().map(|d| (d.id.as_str(), d)).collect();

        let mut runners = Vec::new();
        let mut joined = std::collections::HashSet::new();
        for m in members
            .iter()
            .filter(|m| m.sandbox_kind == MEMBER_KIND_HOST)
        {
            joined.insert(m.sandbox_ref.clone());
            let daemon = by_id.get(m.sandbox_ref.as_str());
            runners.push(Runner {
                id: m.sandbox_ref.clone(),
                name: daemon
                    .and_then(|d| d.name.clone())
                    .or_else(|| m.device_name.clone())
                    .unwrap_or_else(|| m.sandbox_ref.clone()),
                // No daemon row for a host member means the daemon was
                // unregistered but left in the network. Reporting it as
                // `Orphaned` rather than dropping it is what makes "my runner
                // vanished" answerable.
                status: daemon
                    .map(|d| RunnerStatus::from(d.status))
                    .unwrap_or(RunnerStatus::Orphaned),
                last_seen_at: daemon
                    .map(|d| d.last_seen_at.clone())
                    .or_else(|| m.last_seen_at.clone()),
            });
        }
        runners.sort_by(|a, b| a.name.cmp(&b.name));

        let mut unjoined: Vec<Runner> = daemons
            .iter()
            .filter(|d| !joined.contains(&d.id))
            .map(|d| Runner {
                id: d.id.clone(),
                name: d.name.clone().unwrap_or_else(|| d.id.clone()),
                status: RunnerStatus::from(d.status),
                last_seen_at: Some(d.last_seen_at.clone()),
            })
            .collect();
        unjoined.sort_by(|a, b| a.name.cmp(&b.name));

        (runners, unjoined)
    }

    /// Resolve `CI_NETWORK` — which may be an id or a name — to one network.
    ///
    /// An ambiguous name is an error rather than a first-match, because the two
    /// candidates would have different runner pools and picking one silently is
    /// how a job lands on a machine nobody expected.
    async fn resolve_network(&self, opts: &HeyoClientOptions) -> Result<NetworkInfo, RunnerError> {
        let wanted = self.config.heyvm.network.trim();
        let networks = Network::list(HeyoClientOptions {
            api_key: opts.api_key.clone(),
            base_url: opts.base_url.clone(),
            timeout: opts.timeout,
        })
        .await
        .map_err(|e| RunnerError::ControlPlane(format!("GET /networks: {e}")))?;

        if let Some(exact) = networks.iter().find(|n| n.id == wanted) {
            return Ok(exact.clone());
        }
        let by_name: Vec<&NetworkInfo> = networks
            .iter()
            .filter(|n| n.name.eq_ignore_ascii_case(wanted))
            .collect();
        match by_name.len() {
            1 => Ok(by_name[0].clone()),
            0 => Err(RunnerError::UnknownNetwork {
                wanted: wanted.to_string(),
                available: networks.iter().map(|n| n.name.clone()).collect(),
            }),
            _ => Err(RunnerError::AmbiguousNetwork {
                wanted: wanted.to_string(),
                ids: by_name.iter().map(|n| n.id.clone()).collect(),
            }),
        }
    }

    /// A client whose `base_url` is a live tunnel into `runner_id`'s daemon.
    ///
    /// Cheap on a hit. On a miss it fetches a ticket, dials over iroh, and — the
    /// first time — proves the daemon actually enforces authentication.
    pub async fn client_for(&self, runner_id: &str) -> Result<HeyoClient, RunnerError> {
        let mut cache = self.tunnels.lock().await;
        if let Some(c) = cache.get(runner_id) {
            return Ok(c.clone());
        }

        let ticket = self.connection_ticket(runner_id).await?;
        let client = HeyoClient::connect_p2p(
            &ticket,
            self.config.heyvm.relay.as_deref(),
            Some(self.config.heyvm.api_key.clone()),
        )
        .await
        .map_err(|e| RunnerError::Unreachable {
            runner: runner_id.to_string(),
            reason: format!("iroh connect failed: {e}"),
        })?;

        self.assert_daemon_requires_auth(runner_id, &client).await?;

        cache.insert(runner_id.to_string(), client.clone());
        tracing::info!(runner = runner_id, "opened an iroh tunnel to the daemon");
        Ok(client)
    }

    /// SDK client options pointed at a live tunnel to `runner_id`.
    ///
    /// This is the seam between this module and [`crate::vm`]: tunnels and
    /// credentials live here, and the VM layer only ever sees options. The
    /// returned `base_url` is a local forwarded port that stays open because the
    /// cache above holds the client that owns it — building a second client from
    /// these options rides the same link rather than opening another.
    pub async fn options_for(&self, runner_id: &str) -> Result<HeyoClientOptions, RunnerError> {
        if let Some(url) = &self.config.heyvm.local_runner {
            // A same-machine daemon runs without JWT_SECRET and ignores a
            // bearer, so none is sent — matching `HeyoClient::local`.
            return Ok(HeyoClientOptions {
                api_key: None,
                base_url: Some(url.clone()),
                timeout: None,
            });
        }
        let client = self.client_for(runner_id).await?;
        Ok(HeyoClientOptions {
            api_key: Some(self.config.heyvm.api_key.clone()),
            base_url: Some(client.base_url().to_string()),
            timeout: None,
        })
    }

    /// Drop a runner's tunnel so the next `client_for` redials.
    ///
    /// Called when a request over the tunnel fails in a way that suggests the
    /// link rather than the request — the daemon restarted, the relay dropped
    /// it, the host rebooted.
    pub async fn evict(&self, runner_id: &str) {
        if self.tunnels.lock().await.remove(runner_id).is_some() {
            tracing::info!(runner = runner_id, "evicted the daemon tunnel");
        }
    }

    /// `GET /me/daemons/{id}/connection-ticket`.
    ///
    /// Not on `Daemons` in the SDK, so it goes through the raw client. Uses
    /// `raw_request` rather than `request` to keep the status code: the SDK
    /// folds every 4xx that is not 401/403/404 into one variant, and the
    /// difference between "this daemon has no tunnel yet" (409, normal while a
    /// host is booting) and "no such daemon" (404, a configuration error) is
    /// exactly what an operator needs to see.
    async fn connection_ticket(&self, runner_id: &str) -> Result<String, RunnerError> {
        let client = HeyoClient::new(self.client_options()).map_err(|e| {
            RunnerError::ControlPlane(format!("could not build a cloud client: {e}"))
        })?;
        let path = format!("/me/daemons/{runner_id}/connection-ticket");
        let response = client
            .raw_request(Method::GET, &path, None::<&()>, RequestOptions::default())
            .await
            .map_err(|e| RunnerError::Unreachable {
                runner: runner_id.to_string(),
                reason: format!("fetching a connection ticket: {e}"),
            })?;

        let status = response.status();
        let body: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);

        if !status.is_success() {
            let detail = body
                .get("message")
                .or_else(|| body.get("error"))
                .and_then(|v| v.as_str())
                .unwrap_or("no detail")
                .to_string();
            return Err(match status.as_u16() {
                409 => RunnerError::NoTicket {
                    runner: runner_id.to_string(),
                    detail,
                },
                404 => RunnerError::UnknownRunner(runner_id.to_string()),
                other => RunnerError::Unreachable {
                    runner: runner_id.to_string(),
                    reason: format!("connection-ticket returned {other}: {detail}"),
                },
            });
        }

        body.get("connectionUrl")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| RunnerError::NoTicket {
                runner: runner_id.to_string(),
                detail: "the cloud returned an empty connectionUrl".to_string(),
            })
    }

    /// Prove the daemon rejects an unauthenticated request before trusting the
    /// tunnel with anything.
    ///
    /// A daemon started without `JWT_SECRET` (and without an internal API key)
    /// disables its auth middleware entirely — `mvm-ctrl/src/api.rs:885` passes
    /// every request through. Combined with a bearer-equivalent ticket, that
    /// means anyone who ever saw the ticket has the host. Probing costs one
    /// round trip per tunnel and turns a silent exposure into a startup error.
    ///
    /// `CI_ALLOW_UNAUTHENTICATED_RUNNERS=true` downgrades it to a warning, for a
    /// single-machine development loop where the tunnel never leaves localhost.
    async fn assert_daemon_requires_auth(
        &self,
        runner_id: &str,
        tunneled: &HeyoClient,
    ) -> Result<(), RunnerError> {
        // Same local port, no bearer. `local_at` exists for exactly this: a
        // client pointed at a port somebody else bound.
        let anon = match HeyoClient::local_at(tunneled.base_url()) {
            Ok(c) => c,
            Err(e) => {
                // Not being able to build the probe is not evidence either way,
                // so it must not read as a pass.
                return Err(RunnerError::Unreachable {
                    runner: runner_id.to_string(),
                    reason: format!("could not build an auth probe: {e}"),
                });
            }
        };

        let probe = anon
            .raw_request(
                Method::GET,
                "/sandboxes",
                None::<&()>,
                RequestOptions {
                    timeout: Some(Duration::from_secs(10)),
                    query: Vec::new(),
                },
            )
            .await;

        let open = match probe {
            // A protected route answering without a bearer is the failure.
            Ok(r) => r.status().is_success(),
            // A transport error proves nothing about auth; let the real request
            // report it rather than failing here for the wrong reason.
            Err(_) => return Ok(()),
        };

        if !open {
            return Ok(());
        }
        if self.config.allow_unauthenticated_runners {
            tracing::warn!(
                runner = runner_id,
                "this daemon serves its API without authentication; its iroh ticket \
                 is equivalent to a host shell. Set JWT_SECRET on the runner."
            );
            return Ok(());
        }
        Err(RunnerError::DaemonUnauthenticated(runner_id.to_string()))
    }

    /// Refresh now, then on `CI_RUNNER_REFRESH_SECS`, until the process ends.
    ///
    /// The first refresh is awaited by the caller so startup can report a
    /// broken network immediately; failures after that are logged and retried
    /// rather than fatal, because a cloud blip is not a reason to stop serving
    /// a dashboard.
    pub fn spawn_refresh_loop(self: Arc<Self>) {
        let interval = self.config.heyvm.refresh_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Err(e) = self.refresh().await {
                    tracing::warn!("runner refresh failed, keeping the last snapshot: {e}");
                }
            }
        });
    }
}

#[derive(Debug)]
pub enum RunnerError {
    ControlPlane(String),
    UnknownNetwork {
        wanted: String,
        available: Vec<String>,
    },
    AmbiguousNetwork {
        wanted: String,
        ids: Vec<String>,
    },
    UnknownRunner(String),
    NoTicket {
        runner: String,
        detail: String,
    },
    Unreachable {
        runner: String,
        reason: String,
    },
    DaemonUnauthenticated(String),
}

impl fmt::Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlPlane(msg) => write!(f, "heyvm control plane: {msg}"),
            Self::UnknownNetwork { wanted, available } => {
                if available.is_empty() {
                    write!(
                        f,
                        "CI_NETWORK={wanted:?} matches no network, and this account has none. \
                         Create one with `heyvm network create`."
                    )
                } else {
                    write!(
                        f,
                        "CI_NETWORK={wanted:?} matches no network. Available: {}",
                        available.join(", ")
                    )
                }
            }
            Self::AmbiguousNetwork { wanted, ids } => write!(
                f,
                "CI_NETWORK={wanted:?} matches {} networks ({}); set it to an id instead, \
                 because the two have different runner pools",
                ids.len(),
                ids.join(", ")
            ),
            Self::UnknownRunner(id) => write!(
                f,
                "no daemon {id:?} is registered to this account; it may have been \
                 unregistered while still a member of the network"
            ),
            Self::NoTicket { runner, detail } => write!(
                f,
                "runner {runner:?} has no P2P connection ticket ({detail}). It is \
                 registered but its heyvmd tunnel is not up — check that heyvmd is \
                 running on that host."
            ),
            Self::Unreachable { runner, reason } => {
                write!(f, "could not reach runner {runner:?}: {reason}")
            }
            Self::DaemonUnauthenticated(id) => write!(
                f,
                "runner {id:?} serves its heyvm API without authentication, so its \
                 iroh ticket grants a host shell to anyone who has seen it. Start \
                 heyvmd with JWT_SECRET set, or set \
                 CI_ALLOW_UNAUTHENTICATED_RUNNERS=true if this host is local-only."
            ),
        }
    }
}

impl std::error::Error for RunnerError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn runner(id: &str, name: &str, status: RunnerStatus) -> Runner {
        Runner {
            id: id.into(),
            name: name.into(),
            status,
            last_seen_at: None,
        }
    }

    #[test]
    fn a_runner_is_addressable_by_id_or_by_name() {
        let r = runner("hd-abc123", "bigbox", RunnerStatus::Online);
        assert!(r.matches("hd-abc123"));
        assert!(r.matches("bigbox"));
        assert!(r.matches("BigBox"), "names are case-insensitive");
        assert!(!r.matches("hd-ABC123"), "ids are matched exactly");
        assert!(!r.matches("smallbox"));
    }

    /// Only `Online` may take work. A stale daemon still has a row and a
    /// tunnel ticket, so dispatching to it would hang until the job timed out
    /// rather than failing fast.
    #[test]
    fn only_online_runners_are_dispatchable() {
        assert!(RunnerStatus::Online.is_dispatchable());
        for s in [
            RunnerStatus::Stale,
            RunnerStatus::Offline,
            RunnerStatus::Orphaned,
        ] {
            assert!(!s.is_dispatchable(), "{} must not take work", s.as_str());
        }
    }

    #[test]
    fn a_set_finds_by_either_spelling_and_filters_to_dispatchable() {
        let set = RunnerSet {
            network_id: "net-1".into(),
            network_name: "prod-runners".into(),
            runners: vec![
                runner("hd-1", "bigbox", RunnerStatus::Online),
                runner("hd-2", "oldbox", RunnerStatus::Stale),
                runner("hd-3", "ghost", RunnerStatus::Orphaned),
            ],
            unjoined: vec![],
            last_error: None,
        };
        assert_eq!(set.find("bigbox").unwrap().id, "hd-1");
        assert_eq!(set.find("hd-2").unwrap().name, "oldbox");
        assert!(set.find("nope").is_none());

        let live: Vec<&str> = set.dispatchable().map(|r| r.id.as_str()).collect();
        assert_eq!(live, ["hd-1"]);
    }

    /// A member of kind `host` with no daemon row is the "unregistered but
    /// still in the network" case, and must be visible rather than dropped.
    #[test]
    fn daemon_status_maps_onto_runner_status() {
        assert_eq!(
            RunnerStatus::from(DaemonStatus::Online),
            RunnerStatus::Online
        );
        assert_eq!(RunnerStatus::from(DaemonStatus::Stale), RunnerStatus::Stale);
        assert_eq!(
            RunnerStatus::from(DaemonStatus::Offline),
            RunnerStatus::Offline
        );
    }

    /// Every error has to name what to do about it — these are read by whoever
    /// is staring at a job that will not start.
    #[test]
    fn errors_name_the_fix() {
        let e = RunnerError::UnknownNetwork {
            wanted: "prod".into(),
            available: vec!["default".into(), "lab".into()],
        };
        let s = e.to_string();
        assert!(s.contains("prod"), "{s}");
        assert!(s.contains("default, lab"), "{s}");

        let e = RunnerError::NoTicket {
            runner: "hd-1".into(),
            detail: "not yet fully online".into(),
        };
        assert!(e.to_string().contains("heyvmd is running"), "{e}");

        let e = RunnerError::DaemonUnauthenticated("hd-1".into());
        assert!(e.to_string().contains("JWT_SECRET"), "{e}");
    }

    /// Member and daemon payloads exactly as the stage control plane returned
    /// them on 2026-08-03, so the projection is pinned to the real wire shape
    /// rather than to my reading of the SDK's structs.
    fn members(json: &str) -> Vec<heyo_sdk::NetworkMember> {
        serde_json::from_str(json).expect("members parse")
    }
    fn daemons(json: &str) -> Vec<DaemonInfo> {
        serde_json::from_str(json).expect("daemons parse")
    }

    /// The failure this guards against is the expensive one: a real network is
    /// mostly `deployed` members, because every sandbox joins one. Treating
    /// those as runners would aim jobs at VMs instead of hosts.
    #[test]
    fn only_host_members_become_runners() {
        let m = members(
            r#"[
              {"device_name":null,"last_seen_at":null,"network_id":"net-1",
               "registered_at":"2026-08-03T20:06:39.420241Z",
               "sandbox_kind":"deployed","sandbox_ref":"dep-d9426372"},
              {"device_name":null,"last_seen_at":null,"network_id":"net-1",
               "registered_at":"2026-08-03T19:50:36.461185Z",
               "sandbox_kind":"deployed","sandbox_ref":"dep-7fa5890b"},
              {"device_name":"pop-os","last_seen_at":null,"network_id":"net-1",
               "registered_at":"2026-08-03T19:12:00.840094Z",
               "sandbox_kind":"host","sandbox_ref":"hd-e_QxrdQOWUsfINWO"},
              {"device_name":null,"last_seen_at":null,"network_id":"net-1",
               "registered_at":"2026-08-03T19:00:00.000000Z",
               "sandbox_kind":"local","sandbox_ref":"sb-123"}
            ]"#,
        );
        let d = daemons(
            r#"[{"id":"hd-e_QxrdQOWUsfINWO","name":"pop-os","status":"online",
                 "lastSeenAt":"2026-08-03T23:39:00Z","createdAt":"2026-07-16T14:04:00Z"}]"#,
        );

        let (runners, unjoined) = Runners::project(&m, &d);
        assert_eq!(runners.len(), 1, "only the host member is a runner");
        assert_eq!(runners[0].id, "hd-e_QxrdQOWUsfINWO");
        assert_eq!(runners[0].name, "pop-os");
        assert_eq!(runners[0].status, RunnerStatus::Online);
        assert!(unjoined.is_empty());
    }

    /// The live account on 2026-08-03: members present, but every one of them
    /// `deployed`. Zero runners is the truthful answer, not a bug.
    #[test]
    fn a_network_of_only_deployed_members_yields_no_runners() {
        let m = members(
            r#"[{"device_name":null,"last_seen_at":null,"network_id":"net-1",
                 "registered_at":"2026-08-03T20:06:39.420241Z",
                 "sandbox_kind":"deployed","sandbox_ref":"dep-d9426372"}]"#,
        );
        let (runners, unjoined) = Runners::project(&m, &[]);
        assert!(runners.is_empty());
        assert!(unjoined.is_empty());
    }

    /// A daemon that never joined is the commonest "why is nothing running"
    /// cause, so it has to come back separately rather than be dropped.
    #[test]
    fn a_registered_daemon_outside_the_network_is_reported_as_unjoined() {
        let d = daemons(
            r#"[{"id":"hd-laptop","name":"laptop","status":"online",
                 "lastSeenAt":"2026-08-03T23:39:00Z","createdAt":"2026-07-16T14:04:00Z"}]"#,
        );
        let (runners, unjoined) = Runners::project(&[], &d);
        assert!(runners.is_empty());
        assert_eq!(unjoined.len(), 1);
        assert_eq!(unjoined[0].name, "laptop");
    }

    /// A host member with no daemon row was unregistered but left behind. It
    /// must stay visible — and must not be dispatchable.
    #[test]
    fn a_host_member_with_no_daemon_row_is_orphaned() {
        let m = members(
            r#"[{"device_name":"ghost","last_seen_at":"2026-08-01T00:00:00Z",
                 "network_id":"net-1","registered_at":"2026-08-01T00:00:00Z",
                 "sandbox_kind":"host","sandbox_ref":"hd-gone"}]"#,
        );
        let (runners, _) = Runners::project(&m, &[]);
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].status, RunnerStatus::Orphaned);
        assert!(!runners[0].status.is_dispatchable());
        // Falls back to the member's device name when there is no daemon label.
        assert_eq!(runners[0].name, "ghost");
        assert_eq!(
            runners[0].last_seen_at.as_deref(),
            Some("2026-08-01T00:00:00Z")
        );
    }

    /// With neither a daemon label nor a device name there is still something
    /// to address the runner by, and `uses:` has to be able to name it.
    #[test]
    fn a_nameless_host_falls_back_to_its_daemon_id() {
        let m = members(
            r#"[{"device_name":null,"last_seen_at":null,"network_id":"net-1",
                 "registered_at":"2026-08-01T00:00:00Z",
                 "sandbox_kind":"host","sandbox_ref":"hd-anon"}]"#,
        );
        let (runners, _) = Runners::project(&m, &[]);
        assert_eq!(runners[0].name, "hd-anon");
        assert!(runners[0].matches("hd-anon"));
    }

    /// Daemon ids carry `_` (the live one is `hd-e_QxrdQOWUsfINWO`), and they
    /// are interpolated into NATS subjects and durable consumer names verbatim.
    #[test]
    fn a_real_daemon_id_is_a_valid_subject_token() {
        assert!(crate::config::is_subject_token("hd-e_QxrdQOWUsfINWO"));
    }

    #[test]
    fn an_empty_account_says_so_rather_than_listing_nothing() {
        let e = RunnerError::UnknownNetwork {
            wanted: "prod".into(),
            available: vec![],
        };
        assert!(e.to_string().contains("this account has none"), "{e}");
    }

    fn test_runners(allow_unauthenticated: bool) -> Runners {
        unsafe {
            std::env::set_var("CI_HEYO_API_KEY", "test-key");
            std::env::set_var("CI_NETWORK", "test-net");
            std::env::set_var("CI_DATABASE_URL", "postgres://localhost/ci_test");
            std::env::set_var("CI_WEBHOOK_SECRET", "0123456789abcdef");
            std::env::set_var(
                "CI_ALLOW_UNAUTHENTICATED_RUNNERS",
                if allow_unauthenticated {
                    "true"
                } else {
                    "false"
                },
            );
        }
        let c = Config::from_env().expect("test config resolves");
        Runners::new(Arc::new(c))
    }

    /// Requires a local `heyvmd` on 34099. Run with:
    /// `cargo test -- --ignored auth_probe`
    ///
    /// This is the check that matters most and the one unit tests cannot reach:
    /// a daemon started without `JWT_SECRET` serves its whole API — host shell
    /// included — to anyone holding its iroh ticket, and a ticket is
    /// bearer-equivalent. Verified against the daemon on this machine, which
    /// answers `GET /sandboxes` with 200 and no credential.
    #[tokio::test]
    #[ignore = "needs a local heyvmd on 127.0.0.1:34099"]
    async fn auth_probe_catches_a_daemon_that_serves_without_a_credential() {
        let local = HeyoClient::local().expect("local client");

        let strict = test_runners(false);
        let err = strict
            .assert_daemon_requires_auth("hd-local", &local)
            .await
            .expect_err("an unauthenticated daemon must be refused");
        assert!(
            matches!(err, RunnerError::DaemonUnauthenticated(_)),
            "got {err:?}"
        );
        assert!(err.to_string().contains("JWT_SECRET"), "{err}");

        // The escape hatch exists for a local-only loop, and must downgrade the
        // refusal to a warning rather than change what was detected.
        let lax = test_runners(true);
        lax.assert_daemon_requires_auth("hd-local", &local)
            .await
            .expect("the opt-out permits it");
    }

    /// A daemon that cannot be reached at all proves nothing about its auth, so
    /// the probe must not report it as authenticated *or* as open — it defers to
    /// the real request, which will fail with a transport error that says so.
    #[tokio::test]
    async fn an_unreachable_daemon_does_not_read_as_a_failed_auth_check() {
        // Port 1 on loopback: nothing listens, and connecting fails fast.
        let dead = HeyoClient::local_at("http://127.0.0.1:1").expect("client");
        test_runners(false)
            .assert_daemon_requires_auth("hd-dead", &dead)
            .await
            .expect("a transport error is not evidence of open auth");
    }
}
