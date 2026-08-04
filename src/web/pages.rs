//! Server-rendered pages.
//!
//! maud rather than a runtime template engine, matching vault and artifacts. The
//! reason is not taste: a placeholder that never gets filled is a compile error
//! here, where app-lb's `include_str!` + `str::replace` approach needs a test
//! asserting no `{{` survives rendering.
//!
//! Pages are self-contained — the stylesheet is inline and there are no external
//! assets. Everything in this ecosystem gets read over an SSH tunnel sooner or
//! later, and a dashboard that needs a CDN is a dashboard that is blank exactly
//! when someone is debugging.

use crate::runners::{Runner, RunnerSet, RunnerStatus};
use crate::store::{ArtifactRow, JobRow, Run, StepRow};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use std::time::Duration;

/// Inline stylesheet. Dark and light both come from `prefers-color-scheme`;
/// nothing here needs a toggle.
const STYLE: &str = r#"
:root {
  --bg: #ffffff; --fg: #16181d; --muted: #666e7a; --line: #e3e6ea;
  --accent: #2b5cff; --ok: #1a7f37; --warn: #9a6700; --bad: #cf222e; --idle: #6e7781;
  --card: #f7f8fa;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117; --fg: #e6edf3; --muted: #8b949e; --line: #23262d;
    --accent: #6b8afd; --ok: #3fb950; --warn: #d29922; --bad: #f85149; --idle: #6e7681;
    --card: #161b22;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--bg); color: var(--fg);
  font: 15px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
}
header {
  display: flex; align-items: baseline; gap: 1.5rem;
  padding: 1rem 1.5rem; border-bottom: 1px solid var(--line);
}
header h1 { font-size: 1rem; margin: 0; font-weight: 650; letter-spacing: -0.01em; }
header nav { display: flex; gap: 1rem; }
header nav a { color: var(--muted); text-decoration: none; }
header nav a:hover, header nav a.on { color: var(--fg); }
header .who { margin-left: auto; color: var(--muted); font-size: 0.875rem; }
main { padding: 1.5rem; max-width: 72rem; }
h2 { font-size: 0.95rem; margin: 0 0 0.75rem; font-weight: 650; }
.sub { color: var(--muted); font-size: 0.875rem; margin: -0.5rem 0 1rem; }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
th, td { text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid var(--line); }
th { color: var(--muted); font-weight: 500; font-size: 0.8rem; text-transform: uppercase;
     letter-spacing: 0.04em; white-space: nowrap; }
td.mono, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85rem; }
.pill {
  display: inline-block; padding: 0.05rem 0.5rem; border-radius: 999px;
  font-size: 0.78rem; font-weight: 550; border: 1px solid currentColor;
}
.pill.online { color: var(--ok); } .pill.stale { color: var(--warn); }
.pill.offline { color: var(--idle); } .pill.orphaned { color: var(--bad); }
.banner {
  padding: 0.7rem 0.9rem; border-radius: 6px; margin-bottom: 1.25rem;
  background: var(--card); border-left: 3px solid var(--bad); font-size: 0.9rem;
}
.empty { color: var(--muted); padding: 1.25rem 0; }
section { margin-bottom: 2rem; }
a { color: var(--accent); }
tr.link:hover { background: var(--card); cursor: pointer; }
td a.row { display: block; color: inherit; text-decoration: none; }
.pill.success { color: var(--ok); } .pill.failure { color: var(--bad); }
.pill.running { color: var(--accent); } .pill.queued, .pill.pending { color: var(--idle); }
.pill.skipped, .pill.cancelled { color: var(--idle); }
.meta { color: var(--muted); font-size: 0.85rem; }
.meta code { color: var(--fg); }
h1.page { font-size: 1.15rem; margin: 0 0 0.25rem; font-weight: 650; }
.step { border: 1px solid var(--line); border-radius: 6px; margin-bottom: 0.6rem; }
.step > summary {
  display: flex; align-items: center; gap: 0.6rem; padding: 0.55rem 0.8rem;
  cursor: pointer; list-style: none; font-size: 0.9rem;
}
.step > summary::-webkit-details-marker { display: none; }
.step > summary::before { content: "▸"; color: var(--muted); font-size: 0.75rem; }
.step[open] > summary::before { content: "▾"; }
.step .grow { flex: 1; }
pre.log {
  margin: 0; padding: 0.7rem 0.9rem; border-top: 1px solid var(--line);
  background: var(--card); overflow-x: auto; white-space: pre-wrap;
  word-break: break-word; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.82rem; line-height: 1.45; max-height: 32rem; overflow-y: auto;
}
pre.log:empty::after { content: "(no output)"; color: var(--muted); }
.dag { display: flex; flex-direction: column; gap: 0.35rem; }
.dag .wave { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; }
.dag .needs { color: var(--muted); font-size: 0.8rem; }
"#;

/// Chrome shared by every page.
///
/// `who` is the app-lb identity when there is one. An app-token caller and an
/// ungated deployment both render without it rather than inventing a user.
pub fn layout(app_name: &str, current: &str, who: Option<&str>, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (app_name) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 { (app_name) }
                    nav {
                        a href="/" class=[(current == "runs").then_some("on")] { "Runs" }
                        a href="/runners" class=[(current == "runners").then_some("on")] { "Runners" }
                        a href="/workflows" class=[(current == "workflows").then_some("on")] { "Workflows" }
                    }
                    @if let Some(who) = who {
                        span .who { (who) }
                    }
                }
                main { (body) }
            }
        }
    }
}

fn status_pill(status: RunnerStatus) -> Markup {
    html! { span class={ "pill " (status.as_str()) } { (status.as_str()) } }
}

fn runner_rows(runners: &[Runner]) -> Markup {
    html! {
        @for r in runners {
            tr {
                td { (r.name) }
                td .mono { (r.id) }
                td { (status_pill(r.status)) }
                td .mono { (r.last_seen_at.as_deref().unwrap_or("—")) }
            }
        }
    }
}

/// `GET /runners` — the pool, and why anything missing is missing.
pub fn runners_page(app_name: &str, who: Option<&str>, set: &RunnerSet) -> Markup {
    let body = html! {
        @if let Some(err) = &set.last_error {
            div .banner {
                strong { "The runner pool is stale. " }
                (err)
            }
        }

        section {
            h2 { "Runners" }
            p .sub {
                @if set.network_name.is_empty() {
                    "No network has been resolved yet."
                } @else {
                    "Hosts in network " code .mono { (set.network_name) }
                    " (" code .mono { (set.network_id) } "). "
                    (set.dispatchable().count()) " of " (set.runners.len()) " can take work."
                }
            }
            @if set.runners.is_empty() {
                p .empty {
                    "No host has joined this network. On the machine you want to build on, run "
                    code .mono { "heyvmd" }
                    " and then "
                    code .mono { "heyvm network add-host" }
                    "."
                }
            } @else {
                div .scroll {
                    table {
                        thead { tr { th { "Name" } th { "Daemon" } th { "Status" } th { "Last seen" } } }
                        tbody { (runner_rows(&set.runners)) }
                    }
                }
            }
        }

        // Shown because "my machine isn't in the list" is otherwise a dead end:
        // a registered daemon that never joined the network looks identical to
        // one that was never registered at all.
        @if !set.unjoined.is_empty() {
            section {
                h2 { "Registered but not in this network" }
                p .sub {
                    "These daemons belong to this account but have not joined "
                    code .mono { (set.network_name) }
                    ", so they cannot take work. Add one with "
                    code .mono { "heyvm network add-host" } "."
                }
                div .scroll {
                    table {
                        thead { tr { th { "Name" } th { "Daemon" } th { "Status" } th { "Last seen" } } }
                        tbody { (runner_rows(&set.unjoined)) }
                    }
                }
            }
        }
    };
    layout(app_name, "runners", who, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runner(id: &str, name: &str, status: RunnerStatus) -> Runner {
        Runner {
            id: id.into(),
            name: name.into(),
            status,
            last_seen_at: Some("2026-08-03T22:00:00Z".into()),
        }
    }

    fn populated() -> RunnerSet {
        RunnerSet {
            network_id: "net-1".into(),
            network_name: "prod-runners".into(),
            runners: vec![
                runner("hd-1", "bigbox", RunnerStatus::Online),
                runner("hd-2", "oldbox", RunnerStatus::Stale),
            ],
            unjoined: vec![runner("hd-9", "laptop", RunnerStatus::Online)],
            last_error: None,
        }
    }

    #[test]
    fn the_page_lists_every_runner_with_its_status() {
        let html = runners_page("ci", Some("Sam Currie"), &populated()).into_string();
        assert!(html.contains("bigbox"));
        assert!(html.contains("hd-1"));
        assert!(html.contains(r#"class="pill online""#));
        assert!(html.contains(r#"class="pill stale""#));
        assert!(html.contains("1 of 2 can take work"));
        assert!(html.contains("Sam Currie"));
    }

    /// An unjoined daemon is the single most common "why is nothing running"
    /// cause, so the page must name it and the command that fixes it.
    #[test]
    fn unjoined_daemons_get_their_own_section_naming_the_fix() {
        let html = runners_page("ci", None, &populated()).into_string();
        assert!(html.contains("Registered but not in this network"));
        assert!(html.contains("laptop"));
        assert!(html.contains("heyvm network add-host"));
    }

    #[test]
    fn an_empty_pool_explains_how_to_add_one() {
        let html = runners_page("ci", None, &RunnerSet::default()).into_string();
        assert!(html.contains("No host has joined this network"));
        assert!(html.contains("heyvmd"));
        assert!(!html.contains("Registered but not in this network"));
    }

    /// A refresh failure must not render as an empty-but-healthy pool.
    #[test]
    fn a_stale_snapshot_says_so() {
        let mut set = populated();
        set.last_error = Some("heyvm control plane: GET /networks: timeout".into());
        let html = runners_page("ci", None, &set).into_string();
        assert!(html.contains("The runner pool is stale"));
        assert!(html.contains("GET /networks: timeout"));
    }

    /// An anonymous request (app-token, or no gate) must not render a user chip
    /// — "a token is not a person".
    #[test]
    fn an_anonymous_request_renders_no_identity() {
        let html = runners_page("ci", None, &populated()).into_string();
        assert!(!html.contains(r#"class="who""#));
    }

    /// maud escapes by construction; this pins it, because runner names come
    /// from a daemon's self-reported hostname.
    #[test]
    fn a_hostile_runner_name_is_escaped() {
        let mut set = populated();
        set.runners[0].name = "<script>alert(1)</script>".into();
        let html = runners_page("ci", None, &set).into_string();
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;"));
    }
}

// ---- shared bits --------------------------------------------------------

/// A status pill for a run, job or step. All three vocabularies overlap and the
/// stylesheet colours them the same way, so one helper is honest here.
fn pill(status: &str) -> Markup {
    html! { span class={ "pill " (status) } { (status) } }
}

/// `1m 04s`, or `4.2s`. Deliberately coarse: nobody reads a build duration to
/// the millisecond, and a stable width keeps a table from jittering as it
/// refreshes.
pub fn human_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else if secs > 0 {
        format!("{}.{}s", secs, d.subsec_millis() / 100)
    } else {
        format!("{}ms", d.as_millis())
    }
}

fn short(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// `refs/heads/main` reads as `main`; anything else is left alone so a tag or a
/// raw sha is still recognisable.
fn short_ref(git_ref: &str) -> &str {
    git_ref.strip_prefix("refs/heads/").unwrap_or(git_ref)
}

// ---- runs list ----------------------------------------------------------

/// `GET /` — recent runs.
pub fn runs_page(app_name: &str, who: Option<&str>, runs: &[Run]) -> Markup {
    let body = html! {
        section {
            h2 { "Runs" }
            @if runs.is_empty() {
                p .empty {
                    "Nothing has been submitted yet. From a repository with "
                    code .mono { ".ci/workflows/*.yml" } ", run " code .mono { "git ci" } "."
                }
            } @else {
                div .scroll {
                    table {
                        thead { tr {
                            th { "Workflow" } th { "Ref" } th { "Commit" }
                            th { "Status" } th { "Duration" } th { "Started" } th { "By" }
                        } }
                        tbody {
                            @for r in runs {
                                tr .link {
                                    td { a .row href={ "/runs/" (r.id) } {
                                        (r.workflow_name
                                            .as_deref()
                                            .filter(|n| !n.trim().is_empty())
                                            .unwrap_or_else(|| workflow_label(&r.workflow_id)))
                                    } }
                                    td { (short_ref(&r.git_ref)) }
                                    td .mono { (short(&r.sha, 12)) }
                                    td { (pill(&r.status)) }
                                    td { (r.duration().map(human_duration).unwrap_or_else(|| "—".into())) }
                                    td .mono { (r.created_at.format("%Y-%m-%d %H:%M").to_string()) }
                                    td { (r.actor_email.as_deref().unwrap_or("—")) }
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    layout(app_name, "runs", who, body)
}

// ---- run detail ---------------------------------------------------------

/// `GET /runs/{id}` — one run's jobs, in dependency order, plus its artifacts.
pub fn run_page(
    app_name: &str,
    who: Option<&str>,
    run: &Run,
    jobs: &[JobRow],
    artifacts: &[ArtifactRow],
) -> Markup {
    let body = html! {
        section {
            h1 .page {
                (run.workflow_name
                    .as_deref()
                    .filter(|n| !n.trim().is_empty())
                    .unwrap_or_else(|| workflow_label(&run.workflow_id)))
                " " (pill(&run.status))
            }
            p .meta {
                code .mono { (short(&run.sha, 12)) } " on " code .mono { (short_ref(&run.git_ref)) }
                @if let Some(by) = &run.actor_email { " · " (by) }
                @if let Some(d) = run.duration() { " · " (human_duration(d)) }
                " · " code .mono { (run.workflow_path) }
            }
            @if let Some(err) = &run.error {
                div .banner { (err) }
            }
        }

        section {
            h2 { "Jobs" }
            @if jobs.is_empty() {
                p .empty { "This run planned no jobs." }
            } @else {
                div .scroll {
                    table {
                        thead { tr {
                            th { "Job" } th { "Status" } th { "Runner" }
                            th { "VM" } th { "Needs" } th { "Duration" }
                        } }
                        tbody {
                            @for j in jobs {
                                tr .link {
                                    td { a .row href={ "/runs/" (run.id) "/jobs/" (j.job_key) } {
                                        (j.display)
                                    } }
                                    td { (pill(&j.status)) }
                                    td { (j.runner_hd_id.as_deref().unwrap_or("—")) }
                                    td .mono { (j.sandbox_id.as_deref().unwrap_or("—")) }
                                    td .mono { (job_needs(j)) }
                                    td { (job_duration(j)) }
                                }
                            }
                        }
                    }
                }
                @for j in jobs {
                    @if let Some(err) = &j.error {
                        div .banner { strong { (j.display) ": " } (err) }
                    }
                }
            }
        }

        @if !artifacts.is_empty() {
            section {
                h2 { "Artifacts" }
                div .scroll {
                    table {
                        thead { tr { th { "Name" } th { "Sink" } th { "Size" } th { "Location" } } }
                        tbody {
                            @for a in artifacts {
                                tr {
                                    td { (a.name) }
                                    td { (a.sink) }
                                    td { (human_bytes(a.size_bytes)) }
                                    td .mono { (a.uri) }
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    layout(app_name, "runs", who, body)
}

/// `needs:` comes out of the stored plan rather than a column, because it is a
/// property of the plan and duplicating it into the row would let the two drift.
fn job_needs(j: &JobRow) -> String {
    let needs = j
        .plan
        .get("needs")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    if needs.is_empty() {
        "—".to_string()
    } else {
        needs
    }
}

fn job_duration(j: &JobRow) -> String {
    let Some(start) = j.started_at else {
        return "—".to_string();
    };
    let end = j.finished_at.unwrap_or_else(chrono::Utc::now);
    (end - start)
        .to_std()
        .ok()
        .map(human_duration)
        .unwrap_or_else(|| "—".into())
}

fn human_bytes(n: i64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

// ---- job detail ---------------------------------------------------------

/// `GET /runs/{id}/jobs/{key}` — one job's steps, with logs.
///
/// `stream_token` is `Some` while the job could still produce output. The page
/// mints it server-side and hands it to a fifteen-line `EventSource` handler;
/// see [`crate::web::stream`] for why the stream needs its own credential.
pub fn job_page(
    app_name: &str,
    who: Option<&str>,
    run: &Run,
    job: &JobRow,
    steps: &[(StepRow, String)],
    stream_token: Option<&str>,
) -> Markup {
    let body = html! {
        section {
            h1 .page { (job.display) " " (pill(&job.status)) }
            p .meta {
                a href={ "/runs/" (run.id) } { (run.workflow_name.as_deref().unwrap_or(&run.workflow_id)) }
                " · " code .mono { (short(&run.sha, 12)) }
                @if let Some(r) = &job.runner_hd_id { " · runner " code .mono { (r) } }
                @if let Some(s) = &job.sandbox_id { " · vm " code .mono { (s) } }
                @if let Some(f) = &job.fingerprint { " · fingerprint " code .mono { (f) } }
                @if job.attempt > 1 { " · attempt " (job.attempt) }
            }
            @if let Some(err) = &job.error {
                div .banner { (err) }
            }
        }

        section id="steps" {
            @if steps.is_empty() {
                p .empty { "This job has not started any steps yet." }
            }
            @for (step, log) in steps {
                // Open by default when the step failed, because that is the one
                // the reader came for.
                details .step open[step.status == "failure"] {
                    summary {
                        span .grow { (step.name) }
                        @if let Some(code) = step.exit_code {
                            @if code != 0 { span .meta { "exit " (code) } }
                        }
                        span .meta { (step_duration(step)) }
                        (pill(&step.status))
                    }
                    pre .log id={ "log-" (step.idx) } { (log) }
                }
            }
        }

        @if let Some(token) = stream_token {
            (live_log_script(&run.id, &job.job_key, token))
        }
    };
    layout(app_name, "runs", who, body)
}

fn step_duration(s: &StepRow) -> String {
    let Some(start) = s.started_at else {
        return String::new();
    };
    let end = s.finished_at.unwrap_or_else(chrono::Utc::now);
    (end - start)
        .to_std()
        .ok()
        .map(human_duration)
        .unwrap_or_default()
}

/// The only script on any page.
///
/// Deliberately not htmx or any other library: every page here has to work over
/// an SSH tunnel with no CDN, so an external asset would make the dashboard
/// blank exactly when someone is debugging. The server sends rendered HTML
/// fragments; this appends them and reloads once the job is done, so the final
/// state is the server's rendering rather than one assembled in the browser.
fn live_log_script(run_id: &str, job_key: &str, token: &str) -> Markup {
    let url = format!(
        "/api/stream/{}/{}?token={}",
        urlencode(run_id),
        urlencode(job_key),
        urlencode(token)
    );
    let js = format!(
        r#"
(function () {{
  var es = new EventSource({url});
  es.addEventListener("log", function (e) {{
    var d = JSON.parse(e.data);
    var pre = document.getElementById("log-" + d.idx);
    if (!pre) {{ location.reload(); return; }}
    var atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 32;
    pre.appendChild(document.createTextNode(d.text));
    if (atBottom) pre.scrollTop = pre.scrollHeight;
  }});
  es.addEventListener("done", function () {{ es.close(); location.reload(); }});
  es.onerror = function () {{ es.close(); }};
}})();
"#,
        url = serde_json::to_string(&url).unwrap_or_else(|_| "\"\"".into())
    );
    html! { script { (PreEscaped(js)) } }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---- workflows ----------------------------------------------------------

/// A workflow's display name.
///
/// `workflow_id` comes from the submit payload and can legitimately be empty —
/// a caller that sends neither `workflowId` nor a repository name. Rendering an
/// empty cell makes that look like a broken row rather than a missing field.
fn workflow_label(id: &str) -> &str {
    if id.trim().is_empty() {
        "(unnamed)"
    } else {
        id
    }
}

/// `GET /workflows` — what this installation knows how to build.
pub fn workflows_page(
    app_name: &str,
    who: Option<&str>,
    workflows: &[(String, Option<Run>)],
    pattern: &str,
) -> Markup {
    let body = html! {
        section {
            h2 { "Workflows" }
            p .sub {
                "Discovered from submitted trees matching " code .mono { (pattern) } "."
            }
            @if workflows.is_empty() {
                p .empty {
                    "Nothing has been submitted yet, so no workflow is known. Run "
                    code .mono { "git ci" } " from a repository that has one."
                }
            } @else {
                div .scroll {
                    table {
                        thead { tr { th { "Workflow" } th { "Last run" } th { "When" } } }
                        tbody {
                            @for (id, last) in workflows {
                                tr {
                                    td { (workflow_label(id)) }
                                    td {
                                        @match last {
                                            Some(r) => a href={ "/runs/" (r.id) } { (pill(&r.status)) },
                                            None => span .meta { "never" },
                                        }
                                    }
                                    td .mono {
                                        @match last {
                                            Some(r) => (r.created_at.format("%Y-%m-%d %H:%M").to_string()),
                                            None => ("—".to_string()),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    layout(app_name, "workflows", who, body)
}

#[cfg(test)]
mod page_tests {
    use super::*;
    use chrono::Utc;

    fn run(status: &str) -> Run {
        Run {
            id: "019fca648a6e-00000000".into(),
            workflow_id: "myapp".into(),
            workflow_path: ".ci/workflows/build.yml".into(),
            workflow_name: Some("build".into()),
            repo_url: "git@example.com:me/app.git".into(),
            git_ref: "refs/heads/main".into(),
            sha: "9183de223817abcdef".into(),
            actor_email: Some("sam@sarocu.com".into()),
            status: status.into(),
            error: None,
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
        }
    }

    fn job(key: &str, status: &str) -> JobRow {
        JobRow {
            id: format!("run.{key}"),
            run_id: "019fca648a6e-00000000".into(),
            job_key: key.into(),
            base_id: key.into(),
            display: key.into(),
            network: None,
            runner_hd_id: Some("hd-local".into()),
            fingerprint: Some("2a99fd001e0b".into()),
            sandbox_id: Some("sb-1a341ac0".into()),
            status: status.into(),
            attempt: 1,
            matrix: serde_json::json!({}),
            outputs: serde_json::json!({}),
            plan: serde_json::json!({ "needs": ["build"] }),
            error: None,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
        }
    }

    fn step(idx: i32, name: &str, status: &str) -> StepRow {
        StepRow {
            id: format!("run.job.{idx}"),
            job_id: "run.job".into(),
            idx,
            name: name.into(),
            uses: None,
            status: status.into(),
            exit_code: Some(if status == "failure" { 1 } else { 0 }),
            operation_id: None,
            log_path: None,
            log_bytes: 0,
            error: None,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
        }
    }

    #[test]
    fn the_runs_page_lists_runs_and_links_to_them() {
        let html = runs_page("ci", Some("Sam"), &[run("success"), run("failure")]).into_string();
        assert!(html.contains("/runs/019fca648a6e-00000000"));
        assert!(html.contains(r#"class="pill success""#));
        assert!(html.contains(r#"class="pill failure""#));
        assert!(html.contains("9183de223817"), "the sha is abbreviated");
        assert!(!html.contains("9183de223817abcdef"), "not the whole sha");
        assert!(html.contains("main"), "refs/heads/ is stripped");
    }

    #[test]
    fn an_empty_dashboard_says_how_to_start() {
        let html = runs_page("ci", None, &[]).into_string();
        assert!(html.contains("git ci"));
    }

    #[test]
    fn the_run_page_shows_jobs_their_needs_and_artifacts() {
        let artifacts = [ArtifactRow {
            name: "dist".into(),
            sink: "disk".into(),
            digest: None,
            size_bytes: 4096,
            uri: "/var/lib/ci/dist".into(),
        }];
        let html = run_page(
            "ci",
            None,
            &run("success"),
            &[job("build", "success"), job("deploy", "skipped")],
            &artifacts,
        )
        .into_string();
        assert!(html.contains("build"));
        assert!(html.contains(r#"class="pill skipped""#));
        assert!(html.contains("hd-local"));
        assert!(html.contains("4.0 KiB"));
        assert!(html.contains("/runs/019fca648a6e-00000000/jobs/build"));
    }

    /// A failing job's error has to be on the run page; otherwise the only way
    /// to find out why a run went red is to open each job in turn.
    #[test]
    fn a_job_error_is_surfaced_on_the_run_page() {
        let mut j = job("build", "failure");
        j.error = Some("step \"Build\" exited 101".into());
        let html = run_page("ci", None, &run("failure"), &[j], &[]).into_string();
        assert!(html.contains("exited 101"), "{html}");
    }

    /// The step the reader came for is the one that failed, so it opens without
    /// a click.
    #[test]
    fn a_failed_step_is_expanded_and_a_passing_one_is_not() {
        let steps = vec![
            (step(0, "Compile", "success"), "compiling\n".to_string()),
            (step(1, "Test", "failure"), "assertion failed\n".to_string()),
        ];
        let html = job_page(
            "ci",
            None,
            &run("failure"),
            &job("build", "failure"),
            &steps,
            None,
        )
        .into_string();
        // Exactly one expanded `<details>`, and it is the failing step's.
        let opened: Vec<&str> = html.matches(r#"class="step" open"#).collect();
        assert_eq!(opened.len(), 1, "expected one expanded step in:\n{html}");
        let open_at = html.find(r#"class="step" open"#).unwrap();
        let compile_at = html.find("Compile").unwrap();
        let test_at = html.find(">Test<").unwrap();
        assert!(
            open_at > compile_at && open_at < test_at,
            "the expanded step must be Test, not Compile"
        );
        assert!(html.contains("assertion failed"));
    }

    /// A finished job gets no token, because nothing needs one.
    #[test]
    fn a_finished_job_page_carries_no_stream_token_or_script() {
        let html = job_page(
            "ci",
            None,
            &run("success"),
            &job("build", "success"),
            &[(step(0, "Compile", "success"), "done\n".into())],
            None,
        )
        .into_string();
        assert!(
            !html.contains("EventSource"),
            "no live stream when it is over"
        );
        assert!(!html.contains("/api/stream/"));
    }

    #[test]
    fn a_running_job_page_wires_up_the_stream() {
        let html = job_page(
            "ci",
            None,
            &run("running"),
            &job("build", "running"),
            &[(step(0, "Compile", "running"), "compiling\n".into())],
            Some("1234.abcd"),
        )
        .into_string();
        assert!(html.contains("EventSource"));
        assert!(html.contains("/api/stream/019fca648a6e-00000000/build?token=1234.abcd"));
        // No external asset: the whole page must work over an SSH tunnel.
        assert!(!html.contains("<script src"), "{html}");
        assert!(!html.contains("http://") || !html.contains("cdn"), "{html}");
    }

    /// Log text is guest output — arbitrary bytes chosen by whatever ran.
    #[test]
    fn log_output_is_escaped() {
        let steps = vec![(
            step(0, "Compile", "success"),
            "<script>alert(1)</script>\n".to_string(),
        )];
        let html = job_page(
            "ci",
            None,
            &run("success"),
            &job("build", "success"),
            &steps,
            None,
        )
        .into_string();
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;"));
    }

    /// A job's display name comes from the workflow file.
    #[test]
    fn a_hostile_job_name_is_escaped() {
        let mut j = job("build", "success");
        j.display = "<img src=x onerror=alert(1)>".into();
        let html = run_page("ci", None, &run("success"), &[j], &[]).into_string();
        assert!(!html.contains("<img src=x"));
        assert!(html.contains("&lt;img"));
    }

    #[test]
    fn durations_read_at_a_glance() {
        assert_eq!(human_duration(Duration::from_millis(420)), "420ms");
        assert_eq!(human_duration(Duration::from_millis(4200)), "4.2s");
        assert_eq!(human_duration(Duration::from_secs(64)), "1m 04s");
        assert_eq!(human_duration(Duration::from_secs(3725)), "1h 02m");
    }

    #[test]
    fn byte_sizes_read_at_a_glance() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(4096), "4.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
    }

    /// A run submitted with neither a workflow id nor a repository name must
    /// not render as an empty cell that reads as a broken row.
    #[test]
    fn an_unnamed_workflow_gets_a_placeholder() {
        let html = workflows_page("ci", None, &[(String::new(), None)], "*.yml").into_string();
        assert!(html.contains("(unnamed)"), "{html}");

        let mut r = run("success");
        r.workflow_id = String::new();
        r.workflow_name = None;
        let html = runs_page("ci", None, &[r]).into_string();
        assert!(html.contains("(unnamed)"), "{html}");
    }

    #[test]
    fn the_workflows_page_lists_each_workflow_once_with_its_latest_run() {
        let html = workflows_page(
            "ci",
            None,
            &[
                ("myapp".into(), Some(run("success"))),
                ("other".into(), None),
            ],
            ".ci/workflows/*.yml",
        )
        .into_string();
        assert!(html.contains("myapp"));
        assert!(html.contains("other"));
        assert!(html.contains("never"));
        assert!(html.contains(".ci/workflows/*.yml"));
    }
}
