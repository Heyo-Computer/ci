//! The HTTP surface: a server-rendered dashboard plus the machine endpoints.
//!
//! **Auth is app-lb's job, not ours.** Deployed behind an app-lb `AuthGate`, a
//! browser request arrives carrying `x-auth-request-email` / `-user` / `-name`,
//! which app-lb strips unconditionally before setting, so they cannot be
//! spoofed. This module reads them and never re-implements a login.
//!
//! One consequence shapes every route below: **an app-lb gate admits browsers
//! and nothing else.** The split is `Accept: text/html`, so curl, `git ci`, and
//! a page's own `EventSource` all get `401 {"error":"authentication
//! required"}`. Machine routes therefore live under `/api` and are listed in the
//! deployment's `public_paths`, each carrying its own credential — the submit
//! endpoint an HMAC, the log stream a short-TTL run-scoped token minted by the
//! page that opens it.

pub mod identity;
pub mod pages;
pub mod stream;

use crate::config::Config;
use crate::dispatch::Dispatcher;
use crate::runners::Runners;
use crate::store::Store;
use crate::trigger;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use identity::Identity;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub runners: Arc<Runners>,
    pub store: Store,
    pub dispatcher: Arc<Dispatcher>,
}

pub fn router(
    config: Arc<Config>,
    runners: Arc<Runners>,
    store: Store,
    dispatcher: Arc<Dispatcher>,
) -> Router {
    // The submit body carries a whole source tree, so it needs its own limit —
    // axum defaults to 2 MiB, which no real repository fits in. The real ceiling
    // is `CI_MAX_SOURCE_BYTES`, checked after decoding; this one just keeps a
    // hostile body from being buffered first. Base64 inflates by 4/3, plus room
    // for the JSON envelope.
    let submit_limit = config.max_source_bytes.saturating_mul(4) / 3 + (1 << 20);

    let state = AppState {
        config,
        runners,
        store,
        dispatcher,
    };

    Router::new()
        // Unauthenticated by design and listed in `public_paths`: app-lb probes
        // it after an `update` job to decide whether the service actually came
        // back, so a gate in front of it would make every deploy look failed.
        .route("/healthz", get(healthz))
        .route("/", get(runs_page))
        .route("/runs/{run_id}", get(run_page))
        .route("/runs/{run_id}/jobs/{job_key}", get(job_page))
        .route("/runners", get(runners_page))
        .route("/workflows", get(workflows_page))
        // In `public_paths`, because an `EventSource` sends
        // `Accept: text/event-stream` and app-lb's gate admits only
        // `text/html`. It carries its own run-scoped token instead.
        .route("/api/stream/{run_id}/{job_key}", get(log_stream))
        .route(
            "/api/submit",
            post(submit).layer(DefaultBodyLimit::max(submit_limit)),
        )
        .with_state(state)
}

/// `POST /api/submit` — what `git ci` calls.
///
/// Takes `Bytes`, not `Json<SubmitRequest>`, and that ordering is the point: the
/// signature is verified over the raw body **before** anything parses it. With a
/// `Json` extractor, axum would deserialize an unauthenticated body first, and
/// the signature check would be guarding a decision already made.
async fn submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let signature = headers
        .get(trigger::SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok());

    if let Err(e) = trigger::verify_signature(&state.config.webhook_secret, &body, signature) {
        // Logged at debug, not warn: an unauthenticated public endpoint gets
        // scanned, and a warn per probe is how a log becomes unreadable.
        tracing::debug!("rejected an unsigned submit: {e}");
        return error(e.status(), &e.to_string());
    }

    let req: trigger::SubmitRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error(StatusCode::BAD_REQUEST, &format!("malformed submit: {e}")),
    };

    // Present only when a browser reached this through app-lb's gate; `git ci`
    // arrives with the HMAC and no identity, so the payload's `pusher` is the
    // fallback.
    let who = Identity::from_headers(&headers);

    match state.dispatcher.submit(&req, who.as_ref()).await {
        Ok(run_ids) => {
            tracing::info!(
                "accepted a submit for {} ({}): {} run(s)",
                req.repository.name,
                req.branch(),
                run_ids.len()
            );
            (
                StatusCode::ACCEPTED,
                axum::Json(serde_json::json!({
                    "runs": run_ids,
                    "url": format!("{}/", state.config.public_url),
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!("submit failed: {e}");
            error(StatusCode::BAD_REQUEST, &e.to_string())
        }
    }
}

fn error(status: StatusCode, message: &str) -> axum::response::Response {
    (status, axum::Json(serde_json::json!({ "error": message }))).into_response()
}

async fn healthz() -> &'static str {
    "ok\n"
}

/// How many runs the landing page shows. A dashboard is for "what happened
/// recently"; anything older is a query, not a scroll.
const RECENT_RUNS: i64 = 50;

fn who_of(headers: &HeaderMap) -> Option<Identity> {
    Identity::from_headers(headers)
}

async fn runs_page(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let who = who_of(&headers);
    match state.store.recent_runs(RECENT_RUNS).await {
        Ok(runs) => pages::runs_page(&state.config.name, who.as_ref().map(|i| i.display()), &runs)
            .into_response(),
        Err(e) => page_error(&state, who.as_ref(), &e.to_string()),
    }
}

async fn run_page(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let who = who_of(&headers);
    let name = who.as_ref().map(|i| i.display());
    match state.store.get_run(&run_id).await {
        Ok(Some(run)) => {
            let jobs = state.store.jobs_of(&run_id).await.unwrap_or_default();
            let artifacts = state.store.artifacts_of(&run_id).await.unwrap_or_default();
            pages::run_page(&state.config.name, name, &run, &jobs, &artifacts).into_response()
        }
        Ok(None) => not_found(&state, who.as_ref(), &format!("No run {run_id}.")),
        Err(e) => page_error(&state, who.as_ref(), &e.to_string()),
    }
}

async fn job_page(
    State(state): State<AppState>,
    Path((run_id, job_key)): Path<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let who = who_of(&headers);
    let name = who.as_ref().map(|i| i.display());

    let Ok(Some(run)) = state.store.get_run(&run_id).await else {
        return not_found(&state, who.as_ref(), &format!("No run {run_id}."));
    };
    let jobs = state.store.jobs_of(&run_id).await.unwrap_or_default();
    let Some(job) = jobs.into_iter().find(|j| j.job_key == job_key) else {
        return not_found(
            &state,
            who.as_ref(),
            &format!("Run {run_id} has no job {job_key}."),
        );
    };

    let mut steps = Vec::new();
    for step in state.store.steps_of(&job.id).await.unwrap_or_default() {
        let log = state.store.read_log(&step).await.unwrap_or_default();
        steps.push((step, log));
    }

    // Only mint a token while there is still something to stream. A finished
    // job gets a static page, and no credential is handed out that nothing
    // needs.
    let token = (!matches!(
        job.status.as_str(),
        "success" | "failure" | "skipped" | "cancelled"
    ))
    .then(|| stream::mint(&state.config, &run_id, &job_key));

    pages::job_page(
        &state.config.name,
        name,
        &run,
        &job,
        &steps,
        token.as_deref(),
    )
    .into_response()
}

async fn workflows_page(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let who = who_of(&headers);
    let name = who.as_ref().map(|i| i.display());
    match state.store.recent_runs(500).await {
        Ok(runs) => {
            // Newest first already, so the first sighting of an id is its most
            // recent run.
            let mut seen: Vec<(String, Option<crate::store::Run>)> = Vec::new();
            for r in runs {
                if !seen.iter().any(|(id, _)| *id == r.workflow_id) {
                    seen.push((r.workflow_id.clone(), Some(r)));
                }
            }
            pages::workflows_page(
                &state.config.name,
                name,
                &seen,
                &state.config.default_workflow_path,
            )
            .into_response()
        }
        Err(e) => page_error(&state, who.as_ref(), &e.to_string()),
    }
}

/// Tail a job's step logs.
///
/// Emits **rendered text appended to a specific step**, not a JSON model the
/// browser assembles into markup — the page stays server-rendered, and the
/// fifteen lines of script only append what arrived. On completion it sends
/// `done`, and the browser reloads so the final state is the server's rendering
/// rather than one stitched together client-side.
async fn log_stream(
    State(state): State<AppState>,
    Path((run_id, job_key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let token = q.get("token").cloned().unwrap_or_default();
    if !stream::verify(&state.config, &token, &run_id, &job_key) {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({
                "error": "this log stream token is missing, expired, or for another job"
            })),
        )
            .into_response();
    }

    let stream = async_stream::stream! {
        // Byte offsets already sent, per step index. A step's log only ever
        // grows, so an offset is all the state a tail needs.
        let mut sent: HashMap<i32, usize> = HashMap::new();

        loop {
            let Ok(jobs) = state.store.jobs_of(&run_id).await else { break };
            let Some(job) = jobs.into_iter().find(|j| j.job_key == job_key) else { break };

            for step in state.store.steps_of(&job.id).await.unwrap_or_default() {
                let text = state.store.read_log(&step).await.unwrap_or_default();
                let already = *sent.get(&step.idx).unwrap_or(&0);
                if text.len() > already {
                    // Split on a character boundary: a log is arbitrary bytes
                    // and slicing mid-codepoint would panic.
                    let mut cut = already.min(text.len());
                    while cut < text.len() && !text.is_char_boundary(cut) {
                        cut += 1;
                    }
                    let fresh = &text[cut..];
                    if !fresh.is_empty() {
                        let payload = serde_json::json!({ "idx": step.idx, "text": fresh });
                        yield Ok::<Event, std::convert::Infallible>(
                            Event::default().event("log").data(payload.to_string()),
                        );
                    }
                    sent.insert(step.idx, text.len());
                }
            }

            if matches!(
                job.status.as_str(),
                "success" | "failure" | "skipped" | "cancelled"
            ) {
                yield Ok(Event::default().event("done").data("{}"));
                break;
            }
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
    };

    Sse::new(stream)
        // A proxy that sees nothing for a minute will close the connection;
        // a comment every fifteen seconds keeps it open through a slow step.
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

fn page_error(state: &AppState, who: Option<&Identity>, message: &str) -> axum::response::Response {
    tracing::warn!("page render failed: {message}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        pages::layout(
            &state.config.name,
            "",
            who.map(|i| i.display()),
            maud::html! { div .banner { (message) } },
        ),
    )
        .into_response()
}

fn not_found(state: &AppState, who: Option<&Identity>, message: &str) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        pages::layout(
            &state.config.name,
            "",
            who.map(|i| i.display()),
            maud::html! {
                div .banner { (message) }
                p { a href="/" { "Back to runs" } }
            },
        ),
    )
        .into_response()
}

async fn runners_page(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let who = Identity::from_headers(&headers);
    pages::runners_page(
        &state.config.name,
        who.as_ref().map(|i| i.display()),
        &state.runners.snapshot(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    /// Binds no port — `oneshot` drives the router directly.
    pub(crate) fn test_config() -> Arc<Config> {
        // Set the required vars for a config that validates, then drop them so
        // tests stay independent of each other's environment.
        unsafe {
            std::env::set_var("CI_HEYO_API_KEY", "test-key");
            std::env::set_var("CI_NETWORK", "test-net");
            std::env::set_var("CI_DATABASE_URL", "postgres://localhost/ci_test");
            std::env::set_var("CI_WEBHOOK_SECRET", "0123456789abcdef");
        }
        let c = Config::from_env().expect("test config resolves");
        Arc::new(c)
    }

    /// A pool that has never refreshed. No network call is made — `Runners`
    /// only dials when `refresh` or `client_for` is called.
    fn test_runners(config: Arc<Config>) -> Arc<Runners> {
        Arc::new(Runners::new(config))
    }

    /// A router wired to a real database.
    ///
    /// The pages read run history, so there is no honest way to exercise them
    /// without a store; a mock would test the mock.
    async fn test_router() -> Router {
        let url = std::env::var("CI_TEST_DATABASE_URL").expect("CI_TEST_DATABASE_URL");
        let config = test_config();
        let dir = std::env::temp_dir().join(format!("ci-web-logs-{}", crate::vm::new_id()));
        let store = Store::connect(&url, dir).await.expect("store");
        store
            .migrate(std::path::Path::new("migrations"))
            .await
            .expect("migrations");
        let runners = test_runners(config.clone());
        let dispatcher = Arc::new(Dispatcher {
            config: config.clone(),
            store: store.clone(),
            pool: crate::pool::Pool::new(store.pool().clone()),
            bus: Arc::new(
                // A fixed prefix, not a fresh one per test: these tests never
                // publish, so sharing one stream pair is harmless, and minting
                // a new pair per run leaves a NATS littered with them.
                crate::bus::Bus::connect(&config.nats, "citestweb")
                    .await
                    .expect("nats"),
            ),
            runners: runners.clone(),
            vms: Arc::new(crate::vm::Vms::new()),
            secrets: crate::secrets::Secrets::new(&config),
            artifacts: Arc::from(crate::artifacts::sink_for(&config).expect("disk sink")),
            objects: Arc::new(crate::objects::Workflows::new(&config)),
        });
        router(config, runners, store, dispatcher)
    }

    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn healthz_answers_without_a_credential() {
        let app = test_router().await;
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    /// The pool page must render before the first refresh lands, because that
    /// is exactly when someone is looking at it — a cold start with a cloud
    /// that has not answered yet.
    #[tokio::test]
    #[ignore = "needs CI_TEST_DATABASE_URL"]
    async fn the_runners_page_renders_with_an_empty_pool() {
        let app = test_router().await;
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/runners")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("No host has joined this network"), "{html}");
    }
}
