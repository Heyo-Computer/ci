# ci

CI orchestration and a server-rendered dashboard for [heyvm](https://heyo.computer)
microVMs, with NATS JetStream as the job queue.

A machine becomes a runner by running `heyvmd` and joining a heyvm network.
There is no agent to install: this process discovers those hosts, opens an iroh
tunnel to each, and drives builds on them.

It is a sibling of [app-lb](../app-lb) and [queue-fn](../queue-fn) — same
conventions, same house style, different problem. app-lb keeps a pool of VMs warm
behind an HTTP data plane; queue-fn runs a command in one per event; `ci` runs a
workflow's worth of them per commit.

## Requirements

- **heyvmd** on every machine that should build, joined to one heyvm network
  (`heyvm network add-host`). `firecracker` and `kvm` are the supported drivers;
  `libvirt` is rejected at parse time.
- **Postgres** for run history, the job DAG and the VM pool.
- **NATS with JetStream** (`nats-server -js -sd /var/lib/ci-js`).
- Optionally **app-lb** for workflow objects and sign-in, **heyosecret** for
  secrets, and the **artifacts** store.

## Run

```bash
CI_HEYO_API_KEY=… CI_NETWORK=prod-runners \
CI_DATABASE_URL=postgres://…/ci CI_WEBHOOK_SECRET=$(openssl rand -hex 32) \
cargo run
```

Configuration is environment-only; there are no CLI arguments. **A
misconfiguration is a startup exit, not a degraded service** — every error names
the variable to fix. See `deploy/supervisor/ci.conf` for the full set.

## Submitting a build

```bash
./install-git-ci.sh                      # installs `git-ci` onto PATH
git config ci.endpoint https://ci.us2.heyo.work
git config ci.secret   <the server's CI_WEBHOOK_SECRET>

git ci --dry-run    # show what would be sent
git ci              # submit HEAD
git ci --dirty      # include uncommitted tracked changes
```

`git ci` sends a **`git archive` tarball of the tree**, signed with HMAC-SHA256.
Two consequences, and both are the point:

- **No repository credential exists anywhere in this system.** Not on the
  orchestrator, not in a guest. The submitter already had read access — they ran
  `git archive` — so nothing else needs its own. A CI system that clones for you
  is a CI system holding a key to every repository it builds.
- **The tree is exactly what the submitter meant.** No re-resolving a ref that
  may have moved, no guessing whether dirty work was included.

The cost: the guest gets a tree with no `.git`, so `git describe` does not work
in a step. The commit and ref arrive as environment variables instead.

## A workflow

```yaml
name: build
on: [submit]

jobs:
  build:
    uses: prod-runners/bigbox        # <network>/<runner>
    vm:
      driver: firecracker
      image: ubuntu:24.04
      size_class: medium
      setup_hooks:
        - apt-get update && apt-get install -y build-essential
      cache_key_files:               # busts the warm VM when these change
        - Cargo.lock
        - rust-toolchain.toml
    strategy:
      matrix:
        target: [x86_64, aarch64]
      max-parallel: 2
    steps:
      - name: Build
        run: cargo build --release --target ${{ matrix.target }}
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          REGION: ${{ vars.REGION }}
      - uses: ci/upload-artifact
        with: { name: bin-${{ matrix.target }}, path: target/release/app }

  deploy:
    uses: prod-runners              # any online host in that network
    needs: [build]
    if: ${{ needs.build.result == 'success' }}
    vm: { driver: firecracker, image: ubuntu:24.04 }
    steps:
      - run: ./deploy.sh
```

GitHub Actions' shape, with two departures.

**`uses:` selects a network and a runner**, where GitHub has `runs-on:` selecting
a label. That is the point of the system: a job names the heyvm network and the
host it wants, and membership of that network is what makes a host eligible.
`<network>/<runner>` pins; `<network>` or `<network>/*` takes any online host;
absent inherits the workflow object's network.

**A pinned job does not silently migrate.** If its host is offline the job stays
queued for that host and fails after `CI_RUNNER_WAIT_SECS`, because the warm pool
is host-local — moving the job discards the cache the pin asked for and turns a
fast build into a slow one for reasons nothing reports. `fallback: any` opts in.

**`vm:` describes the machine.** GitHub gives you an opaque runner image; here
the author declares the driver, image, size and setup hooks — and, via
`cache_key_files`, what should invalidate the warm VM the next run would reuse.

`deny_unknown_fields` is on throughout, so `stpes:` or `timeout_minutes:`
(instead of `timeout-minutes:`) is a parse error naming the job, not a field that
quietly does nothing.

## The warm VM pool

```
fingerprint = sha256( canonical_json(vm block, minus cache_key_files)
                    ‖ for each path in sorted(cache_key_files):
                          path ‖ 0x00 ‖ sha256(contents)  — or an ABSENT marker )
```

A job claims an idle VM on its runner with a matching fingerprint, or builds one.
Two decisions in there:

- **`cache_key_files` is stripped before hashing.** Otherwise editing the *list*
  would rebuild every VM even when every listed file is byte-identical.
- **A missing file hashes to an explicit marker.** Skipping it would make "no
  `Cargo.lock`" and "an empty `Cargo.lock`" indistinguishable, so *adding* a
  lockfile later would not bust the pool — the moment it most needs busting.

The pool table survives a restart. Without it a crash orphans every VM until its
TTL, and the next run builds a second pool beside the one already sitting there.

## Workflow objects

```bash
serverctl create workflow build \
  --repo git@github.com:me/app.git \
  --network prod-runners \
  --path '.ci/workflows/*.yml'

serverctl get workflows
```

Stored by app-lb, polled by `ci`. An object points at a repository and a path
glob — the workflow itself lives in the repository it builds, versioned with the
code, so the object is a pointer rather than a copy that can drift.

Objects are matched on the **repository**, because `git ci` knows what it is a
clone of but not what somebody named the object; `git@github.com:me/app.git` and
`https://github.com/me/app` match. Several objects may name one repository —
`build` and `nightly` with different globs is legitimate — and each gets its own
runs, because each is an independent answer to "did this commit pass".

Without `CI_APP_LB_URL`, submits fall back to `CI_WORKFLOW_PATH` and the system
works with no objects at all.

## Secrets

`${{ secrets.X }}` and `${{ vars.X }}` resolve from heyosecret under
`ci/<workflow>/<environment>/`.

**This process is the policy layer, because heyosecret has none.** Its token can
read, write and revoke every secret at every path; `readAccess`/`writeAccess` are
stored and returned but never enforced. So there is no configuration of
heyosecret that makes handing its token to a build safe. The orchestrator holds
it, resolves what a workflow is entitled to, and injects only the values.

heyosecret makes no secret/variable distinction, so the convention is its
`tags[]`: an entry tagged `public` becomes `vars.*` and is left in plain text;
everything else becomes `secrets.*` and is **masked on the write path** — before
a log line is persisted or streamed, so a secret never reaches disk in plain text
for someone to find later.

## Deploying it

A **static `proxy_pass` deployment with an `update` block**, like app-obs — see
`deploy/ci.json`. The orchestrator holds long-lived iroh tunnels and a Postgres
pool, and app-lb's update flow re-probes upstreams after the commands run, so "it
exited 0 but never came back" is a failed deploy rather than a green one.

```bash
serverctl apply -f deploy/ci.json
serverctl update ci
```

Identity comes from app-lb: `x-auth-request-user` (the stable Google `sub`, and
the primary key), `-email`, `-name`. app-lb strips those unconditionally before
setting them, so they are trustworthy — but only on a gated deployment.

**An app-lb gate admits browsers and nothing else.** The split is
`Accept: text/html`, so curl, `git ci`, *and a page's own `EventSource`* all get
`401 {"error":"authentication required"}`. Hence `public_paths` covers
`/api/submit` (which verifies its own HMAC) and `/api/stream/` (which carries a
short-lived, job-scoped token minted by the page that opens it — and that page
was fetched through the gate).

app-lb has no roles, so `ci` keeps its own `ci_user` table keyed on the subject,
seeded from `CI_ADMIN_EMAILS`. Promotion from that list is sticky; dropping off
it does not demote, so a role granted in the UI survives an env change.

## Design notes

### Steps do not use the SDK's exec

`heyo-sdk`'s `Commands::run` posts `{command, cwd, env}` and never sends
`timeout_secs` — `CommandRunOptions::timeout` bounds the *HTTP client*, not the
guest. The firecracker serial path then caps every command at 30 seconds, which
no build survives. So steps go through the daemon's own
`POST /sandboxes/{id}/exec-operations`, which does take a guest timeout.

That route is also **idempotent by `operationId` and persisted**, and step ids are
derived from the run and job key rather than minted. So a JetStream redelivery
re-posts the same step and *reattaches* to the operation already running instead
of building twice.

### Source reaches the guest through exec, in chunks

Neither `Files::write` nor the daemon's upload route reaches a Firecracker guest:
both write into a host-side *mount*, which a sandbox does not have — the call
fails with `Mount not found: /workspace (available mounts: [])`. They also cap at
10 MB. Exec is the only transport that works on every backend.

The chunk size is measured, not chosen. The daemon renders a command as
`env … sh -lc '<script>'`, so the script is one argv entry and Linux's
`MAX_ARG_STRLEN` bounds it. Probed against a real guest: 32 KiB succeeds, 128 KiB
returns `bash: /usr/bin/env: Argument list too long`.

Anything reading *out* of a guest must end its output with a newline. The serial
path frames output with newline-delimited markers, so `base64 -w0` — one
unterminated line — hangs the operation in `running` forever.

### Job subjects are sharded per runner

`WorkQueue` retention deletes on ack, so the stream's depth *is* the backlog. The
cost is that JetStream permits only one consumer per subject, which is why
queue-fn documents itself as single-instance. Sharding the subject by runner
sidesteps it: one durable consumer per runner, filters that never overlap, and
several orchestrators can run at once as long as they own disjoint runner sets.

Two disjoint spaces, and the `r`/`n` segment is load-bearing:

```
<prefix>.job.r.<runner_id>     pinned to one host
<prefix>.job.n.<network_id>    any online host in that network
```

Without it, a network named like a runner would produce overlapping filters and
two consumers would silently eat each other's work.

A queue message carries **ids only**. The expanded plan lives in `ci_job.plan`,
so a redelivery runs exactly what the original delivery would have, even if the
branch moved underneath it.

### Migrations

`migrations/*.sql` are re-executed on every startup with no tracking table —
heyosecret's approach. Every statement must be idempotent; additive changes are
`ALTER TABLE … ADD COLUMN IF NOT EXISTS`, because the `CREATE TABLE` above is a
no-op once the table exists.

Two things make that actually safe. **A Postgres advisory lock**, because
`CREATE TABLE IF NOT EXISTS` is *not* concurrency-safe — two sessions both find
the table absent and the loser dies on `pg_type_typname_nsp_index`, and two
instances starting together is normal here. And **a `lock_timeout` with retries**,
because `ALTER TABLE` needs `ACCESS EXCLUSIVE` on a table a live dispatcher is
inserting into; without a bound, a rolling deploy hangs a starting instance
behind a long build.

### An iroh ticket is bearer-equivalent

`mvm-ctrl/docs/cross-machine-hardening.md` is explicit: the `hey-proxy/tcp/0`
ALPN accepts any peer that knows the ticket, and the daemon cannot verify the
peer. A runner daemon with no `JWT_SECRET` therefore hands a host shell to
anyone who has seen a ticket that may have transited a log. Every tunnel is
probed once, unauthenticated, and a daemon that answers is refused —
`CI_ALLOW_UNAUTHENTICATED_RUNNERS=true` downgrades that to a warning for a
local-only loop.

### Storage

Postgres for runs, jobs, steps, artifacts and the pool; **step logs go to disk**
with the path and byte count on the row. A build log is megabytes, and putting it
in a column means every listing query drags all of it across the wire.

## Tests

```bash
cargo test                                    # unit; no services needed

CI_TEST_DATABASE_URL=postgres://…/ci_test \
CI_TEST_NATS_URL=nats://127.0.0.1:4222 \
  cargo test -- --ignored --test-threads=1    # integration
```

The integration tests want Postgres, NATS with JetStream, and a local `heyvmd`.
The end-to-end test boots a real VM, runs a workflow, proves the VM is reused on
a second run and rebuilt when a `cache_key_files` entry changes, then destroys
it. `CI_TEST_DRIVER=kvm` switches drivers; firecracker is the default because
`kvm` re-execs the daemon's own binary and fails whenever that path has been
rebuilt.

Leftover streams from a run that was killed mid-test:

```bash
CI_TEST_STREAM_PREFIXES=citest cargo test -- --ignored delete_leftover
```

## Status

Working: workflow parsing and planning (matrix, `needs`, `if`, `max-parallel`),
runner discovery, the VM pool, the job queue, `git ci`, secrets with masking,
disk and `artifacts` sinks, the dashboard with live logs, and workflow objects.

Not built yet:

- **The S3 artifact sink.** Declared and selectable; fails loudly naming the
  alternatives rather than reporting an artifact stored that is not there.
- **Composite `uses:` actions.** Only `ci/upload-artifact` is built in. Fetching
  an `action.yml` from a repository is a different feature with a different trust
  model.
- **Triggers other than `submit`.** `on: [schedule]` parses and is reported as
  unsupported rather than silently ignored.
- **`serverctl set workflow`.** Create, get and delete exist; editing means
  re-creating with the same id.
