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
./install-git-submit.sh                  # installs `git-submit` onto PATH

# From the dashboard's /repos page, which mints these two lines for you:
git config ci.endpoint https://ci.us2.heyo.work
git config ci.token    cis_019fca648a6e-00000002.…

git submit --dry-run    # show what would be sent
git submit              # submit HEAD
git submit --dirty      # include uncommitted tracked changes
git submit --archive    # send a tree-only tarball instead of a bundle
```

`git submit` sends a **`git bundle`**, which clones in the guest into a real
repository — so `git describe`, `git log` and `git rev-parse` work in a step. Two
consequences of the submitter packing it rather than the server fetching it, and
both are the point:

- **No repository credential exists anywhere in this system.** Not on the
  orchestrator, not in a guest. The submitter already had read access — they ran
  `git bundle` — so nothing else needs its own. A CI system that clones for you
  is a CI system holding a key to every repository it builds.
- **The tree is exactly what the submitter meant.** No re-resolving a ref that
  may have moved, no guessing whether dirty work was included.

The cost is history. A bundle that clones on its own **must reach a root
commit**: `git bundle create --depth` does not exist, and a `--max-count` slice
is refused at clone time with *"Repository lacks these prerequisite commits"*. So
the payload scales with history rather than with one tree, and `--archive` sends
the old tree-only tarball for the repository where that is the wrong trade.

Two practical requirements: a bundle needs `git` on the orchestrator **and** in
the guest image; a tarball needs neither. Each absence is reported by name.

Three shapes of `git bundle` do not work, and the client is built around them:
it refuses a bare sha (*"Refusing to create empty bundle"*), so `--ref <sha>` and
`--dirty` pack through a throwaway bare repo that borrows your object store via
`alternates` rather than writing refs into it; and a bundle carrying **zero
refs** passes `git bundle verify` as "complete" and clones into an empty
repository, so the server counts refs itself rather than trusting the verify.

## Registered repositories, and the token that submits

```bash
# On the dashboard: /repos → register a clone URL → Mint.
# It shows the token exactly once, with the two `git config` lines above.
```

A submit endpoint on the open internet is arbitrary code execution on a runner,
so what stands in front of it matters more than anything else here. There are two
credentials and the difference is not strength, it is **scope**.

`CI_WEBHOOK_SECRET` is one shared secret, HMAC'd over the body, handed to
everyone who submits from anywhere. It cannot be revoked for one repository, and
it cannot say *which* repository is submitting — so a submit's `repository` field
is something the server takes on trust.

A **repository token** is minted per registration, revocable on its own, and
*is* the statement of which repository the submit is for. A submit whose payload
names a different repository than its token is refused, which is what stops a
token for a repository somebody can push to from building any repository at all
— with this installation's secrets.

- **Stored as a SHA-256 digest, and shown once.** Verifying an HMAC would need
  the server to hold every key, and one read of that table is every repository's
  credential. A bearer inside TLS reverses the trade: the secret transits, and
  what is at rest cannot submit.
- **`ci_repo.workflow_path`** overrides `CI_WORKFLOW_PATH` for one repository. A
  workflow object still wins, being the more specific statement.
- **Pausing** a registration refuses its tokens without destroying them. The
  shared secret is unaffected — it belongs to no repository, so nothing about one
  can stop it. `CI_REQUIRE_REPO_TOKEN=true` turns it off entirely, which is where
  an installation lands once every repository has a token.

`/repos` is deliberately **not** in `public_paths`: it is a browser page behind
app-lb's gate, and admin-only on top of it. With `CI_ADMIN_EMAILS` unset it also
accepts a request carrying no identity at all — that is the local loop, where
there is no gate and no accounts — and startup warns about exactly what that
means.

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

**`uses:` places the job**, where GitHub has `runs-on:` selecting a label. That
is the point of the system: a job names the heyvm network and the machine it
wants, and membership of that network is what makes a host eligible.

```yaml
uses: default                       # the host this CI is running on
uses: prod-runners                  # any online host in that network
uses: prod-runners/bigbox           # that host; `vm:` builds a VM on it
uses: prod-runners/bigbox/sb-1a34   # that existing VM; `vm:` is unused and
                                    # every step is an exec into it
# absent                            # the repository's assigned network, any host
```

**`uses:` carries everything needed to place the job**, and the third form is
why that matters. A sandbox does not record which host it is on — `SandboxInfo`
has no daemon field and there is no cloud-proxied exec — so `<network>/*/<vm>`
would force the orchestrator to interrogate every host in the network to find one
VM. Naming the node is refused-if-absent rather than guessed.

**A named VM is somebody else's machine**, and the executor treats it that way.
It is resolved on the pinned node by id or name, started if it is merely stopped,
and then every step execs into it. Nothing else about the normal path applies:
no fingerprint, no warm pool, no creation — and **no teardown**, so a long-lived
VM is not destroyed because the job's `vm:` block happened to say
`reuse: false`. Its TTL is left alone too; renewing it would be this app quietly
extending the life of something it does not own, which is worth knowing if a
build outlasts the TTL somebody else set.

The `vm:` block is inert for such a job. The schema still requires one — it is a
non-optional field — so it is written and ignored, and the run logs that it was.

`default` is the only form that names no network, and it is not the same as
omitting `uses:`: absent means the repository's assignment, while `default`
means this machine regardless.

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

### This repository's own

`.ci/workflows/build.yml` is one job that produces one thing: the release binary,
uploaded as the `ci` artifact. `cargo test` parses and plans every file in that
directory, so a typo in it fails here rather than at a submit somebody is waiting
on.

**Adding checks.** `cargo fmt --check` and `cargo clippy` are not in it, and the
reason is a trap worth naming: the setup hook installs rustup with
`--profile minimal`, which ships `rustc`, `cargo` and `rust-std` and **not**
rustfmt or clippy. A `cargo fmt` step against that toolchain fails with
`no such command`, which reads as a broken CI rather than a missing component.
Either drop `--profile minimal`, or add the components explicitly:

```yaml
- curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
- . "$HOME/.cargo/env" && rustup component add rustfmt clippy
```

The integration suite is a separate question: it needs Postgres, NATS and a
`heyvmd`, so it belongs on a runner provisioned with them rather than on a
default image.

## Networks

```bash
CI_NETWORK=prod-runners          # serve one
CI_NETWORK=prod-runners,lab      # serve two; the first is the default
CI_NETWORK='*'                   # serve every network on the account
```

`/networks` lists **every** heyvm network on the account with the hosts in each,
whether or not this instance builds for it, plus the daemons that joined no
network at all. That last list is there because "my runner isn't picking up
jobs" is otherwise a dead end: a registered daemon that never joined looks
exactly like one that was never registered.

**Add this host** joins the machine running `ci` to a network, which is
`heyvm network add-host` without needing a shell on that box. It is offered only
for a network this host is not already in, and it is admin-only behind the gate —
joining a host to a network unlocks host-shell access to it, so it is not a read.
Joining does not make this instance *serve* that network; the page says so when
the two differ, because finding out from a job that never runs is worse.

The member is posted by hand rather than through the SDK, because
`NetworkMemberKind` is `Local | Deployed` and has no `host` variant at all —
`heyvm network add-host` has the same problem and solves it the same way.

**What an instance serves is configuration, not discovery.** Jobs are sharded
onto one durable JetStream consumer per runner and per network precisely so
several orchestrators can run at once *as long as they own disjoint sets*. An
instance that silently served everything would eat another's work. `*` opts into
serving everything, which is right for the single instance most installations
run — as a decision that was made rather than one that happened.

Members are read per network, concurrently: the control plane has no
"all members everywhere" route and `NetworkInfo` carries no member count, so N+1
reads is the only shape available. Running them together makes a refresh one
round trip's worth of latency instead of N.

### Assigning one to a repository

A registered repository (`/repos`) can name the network its builds run in, stored
in `ci_repo.network`. The order of precedence, most specific first:

1. the job's own `uses: <network>/<runner>`
2. the workflow object's `network`, where app-lb has one
3. the repository's assigned network
4. the installation default — the first entry of `CI_NETWORK`, or the account's
   default network under `*`

The resolved network is **stamped into the stored job plan at submit time**, not
looked up when the job runs. A redelivery therefore runs where the job was
scheduled, and reassigning a repository mid-build does not move work onto
hardware that never warmed a VM for it — the same reason the expanded plan is
stored rather than recomputed.

**A submit naming a network this instance does not serve is refused at the
client**, with the network and the served list in the message. The alternative is
a run that exists, jobs on a queue nobody consumes, and no answer to "why is my
build stuck" short of reading a table.

The assignment is stored as the network's **name**, not its id: a name is what
`heyvm network create` took, what `uses:` spells, and what the dashboard shows,
so a hand-written query stays readable. The cost is that renaming a network in
heyvm orphans the assignment — which surfaces as a refused submit and a warning
on `/repos`, rather than as a build that quietly moves.

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

Objects are matched on the **repository**, because `git submit` knows what it is a
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
`Accept: text/html`, so curl, `git submit`, *and a page's own `EventSource`* all get
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
runner discovery, the VM pool, the job queue, `git submit` with per-repository tokens, secrets with masking,
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
