# The build image

A Firecracker rootfs with a Rust toolchain, which is what `.ci/workflows/build.yml`
runs in.

**`ci` builds this itself.** The workflow points `vm.build.dockerfile` at this
file, and the first job to land on a host boots a VM from `FROM`, replays the
Dockerfile inside it, and snapshots the result into that host's image catalog as
`ci-img-<hash of this file and its context>`. Later jobs boot straight from it;
editing this file names a new image, so the next run rebuilds. Nothing has to be
run by hand on a runner, and no host needs `docker`.

That is a change. It used to be built out of band:

```bash
heyvm mvm build --local-only -f deploy/image/Dockerfile -c deploy/image \
    -n ci-rust --size-mb 6144
```

— on every machine that would ever run the job, because `ci` passed `vm.image`
straight to the daemon and the daemon resolves a bare name against
`~/.heyo/images/firecracker/{name}.ext4`. A host where that command had never
been run failed every job at VM creation with *"not found locally and no public
base image with that name"*, and nothing recorded the attempt, so it looked
identical to a run no runner had picked up. The command still works and is still
the way to make an image by hand; `heyvm mvm images` lists what a host has.

## What `ci` honours in this file

`FROM`, `RUN`, `ENV`, `WORKDIR` and `COPY`. `CMD`, `ENTRYPOINT`, `EXPOSE` and
`LABEL` are reported as discarded — see below, they were never going to survive
anyway. Anything else is a parse error rather than a silent drop. Multi-stage
builds, `COPY --from=` and `ADD <url>` are refused by name.

`RUN` is an exec-operation in the booted VM rather than a docker layer, so there
is no layer cache: a rebuild re-runs the whole file. That is the trade for
building on a machine that has no docker, and it is why the cache key is the
whole file rather than a per-instruction hash.

## Sizing the rootfs — the open blocker

**`vm.build` cannot build *this* image yet.** A VM created from a public base
image gets whatever rootfs that image ships. Measured against the daemon:

```
$ df -h /            # a firecracker VM created from `ubuntu`
/dev/root       176M  126M   36M  78% /
```

A Rust toolchain is several gigabytes, so the `RUN curl … rustup` below fails on
ENOSPC. `disk_size_gb` in the workflow does not help: that is the *data* disk
mounted at `/var/cache/ci`, and `snapshot-image` copies the **rootfs**, which is
a different file. The daemon's `POST /sandboxes/{id}/resize` grows the workspace
disk (`resize_workspace_disk`), not the rootfs, and there is no create-time
rootfs size option.

So `vm.build` works today for images that add little to their base — which is
what `ci`'s own end-to-end test covers — and not for a toolchain. Until that
changes, this image is built by hand with the `heyvm mvm build --size-mb 6144`
above and named with `image:`.

The fix is small and lives in `mvm-ctrl`, which already has the pieces:
`grow_ext4_image` (`src/linux_vm_image.rs`) does exactly the
`set_len` → `e2fsck -fp` → `resize2fs` dance on an unmounted ext4. Wiring a
`rootfs_size_gb` through `SandboxCreateOptions` to call it on the freshly-copied
rootfs before first boot would lift the cap, and `ci` would then pass the
workflow's own figure through the way it already passes `disk_size_gb`.

## What the pipeline discards, and what that forces

`docker export` flattens a container filesystem. **OCI metadata does not
survive** — `ENTRYPOINT`, `CMD` and, the one that catches people, `ENV`. The VM
boots straight into `/init.sh` through the kernel's `init=` parameter.

So the toolchain is not on `PATH` because of an `ENV` line. It is on `PATH`
because `/etc/profile.d/10-rust.sh` exists and the daemon renders every step as
`env … sh -lc '<script>'` — `-l` makes that a login shell, which reads
`/etc/profile.d`. Symlinks in `/usr/local/bin` cover anything that execs `cargo`
without a shell.

## Why the cache is not in the workspace

`ci` wipes `/workspace` on every checkout — it runs
`find /workspace -mindepth 1 -maxdepth 1 … -exec rm -rf {} +` before unpacking
the source. A `target/` inside it is therefore destroyed once per run, and a
warm VM would buy nothing but the toolchain being pre-installed.

So `CARGO_TARGET_DIR` is `/var/cache/ci/target`, and `init.sh` mounts the
workflow's `disk_size_gb` disk over `/var/cache/ci`. A second run on the same
warm VM relinks instead of recompiling three hundred crates. A cold VM starts
empty, which is correct — that is what the pool's fingerprint is deciding.

`CARGO_HOME` stays on the rootfs at `/usr/local/cargo`, so the registry cache is
sized into the `--size-mb` above rather than the data disk.

## Sizing

`--size-mb 6144` covers the base, the toolchain, `build-essential` and room for
the crate registry to grow. The build cache is not in it — that is the data disk.
Too small shows up as a build failing on no space rather than as anything about
the image, so leave headroom.

## Changing it

Edit it and submit. The image name is the hash of this file and its build
context, so a change names an image no host has and the next run builds it —
and because the warm VM pool keys on the resolved image name, every pooled VM
built from the old rootfs stops matching at the same moment. Both caches bust
together, which is the thing the old hand-built flow could not do: rebuilding
under the same name left warm VMs running the previous rootfs until they aged
out, because the pool fingerprint had not changed.

To force a rebuild without editing anything, delete the file on the host:

```bash
rm ~/.heyo/images/firecracker/ci-img-*.ext4
```

`ci` notices the next create failing, forgets its record of the image, and
builds it again. Images are otherwise never swept — a rootfs is expensive to
rebuild and carries no state from the run that made it.
