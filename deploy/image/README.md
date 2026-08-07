# The `ci-rust` build image

A Firecracker rootfs with a Rust toolchain, which is what `.ci/workflows/build.yml`
runs in.

```bash
heyvm mvm build --local-only -f deploy/image/Dockerfile -c deploy/image \
    -n ci-rust --size-mb 6144
```

Run it on the host that will build — every machine running `heyvmd` has the
`heyvm` CLI, and `--local-only` skips the upload to the cloud. It needs `docker`
on that host, because the pipeline is `docker build → docker export → mke2fs`.

**`ci` never builds an image.** It passes `vm.image` straight through to the
daemon, which resolves a bare name against `~/.heyo/images/firecracker/{name}.ext4`
and, failing that, tries a public base image. `ci-rust` is neither until the
command above has been run, and a job that names it fails at VM creation with
*"not found locally and no public base image with that name"*. Check what a host
has with `heyvm mvm images`.

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

The image is not fingerprinted by the VM pool — `cache_key_files` hashes files in
the *submitted tree*, not the rootfs. Rebuilding under the same name leaves warm
VMs running the old rootfs until they are destroyed, because a pooled VM is
reused on a matching fingerprint and the fingerprint did not change.

Until the dashboard's cache reset lands, the blunt version is to rebuild under a
new name (`ci-rust-2`) and change `image:` in the workflow, which does change the
fingerprint and forces every job onto a fresh VM.
