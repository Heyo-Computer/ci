#!/usr/bin/env bash
#
# Install the `git ci` subcommand.
#
# Ported from heyo/cicd/install-git-submit.sh, which this repository's CI
# replaces. Same two sources as that one:
#
#   local   copy bin/git-ci from this checkout
#   remote  download from the release channel and verify its SHA-256
#
# Defaults to `local` when run from a checkout that has the script, `remote`
# otherwise — which is what makes `curl … | bash` work.
#
#   ./install-git-ci.sh
#   CI_INSTALL_DIR=~/bin ./install-git-ci.sh
#   CI_INSTALL_SOURCE=remote ./install-git-ci.sh
set -euo pipefail

install_dir="${CI_INSTALL_DIR:-$HOME/.local/bin}"
target="$install_dir/git-ci"
base_url="${CI_INSTALL_BASE_URL:-https://heyo-cli-releases.s3.amazonaws.com/git-ci}"

# Piped into bash, `BASH_SOURCE[0]` is unset or `-`, so there is no checkout to
# copy from and remote is the only possible source.
default_source="auto"
if [ -z "${BASH_SOURCE[0]:-}" ] || [ "${BASH_SOURCE[0]:-}" = "-" ]; then
  default_source="remote"
fi
source_mode="${CI_INSTALL_SOURCE:-$default_source}"

local_script=""
if [ "$default_source" != "remote" ]; then
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  [ -f "$here/bin/git-ci" ] && local_script="$here/bin/git-ci"
fi

version_of() {
  [ -f "$1" ] || { echo "not installed"; return 0; }
  sed -n 's/^GIT_CI_VERSION="\([^"]*\)".*/\1/p' "$1" | head -1 || echo unknown
}

download() {
  local dest="$1"
  command -v curl >/dev/null 2>&1 || {
    echo "Error: curl is required to install from ${base_url}" >&2; exit 1; }

  local version
  version="$(curl --fail --show-error --silent --location "${base_url%/}/latest/version.txt" | tr -d '[:space:]')"
  [ -n "$version" ] || { echo "Error: no version manifest at ${base_url}" >&2; exit 1; }

  echo "Downloading git-ci $version"
  curl --fail --show-error --silent --location "${base_url%/}/latest/git-ci" --output "$dest"

  # Verified, not trusted. This script is also the thing people pipe into bash,
  # so the one artifact it fetches has to be checked.
  local expected actual
  expected="$(curl --fail --show-error --silent --location "${base_url%/}/latest/sha256.txt" | awk '{print $1}')"
  if [ -n "$expected" ]; then
    if command -v sha256sum >/dev/null 2>&1; then
      actual="$(sha256sum "$dest" | awk '{print $1}')"
    else
      actual="$(shasum -a 256 "$dest" | awk '{print $1}')"
    fi
    [ "$expected" = "$actual" ] || {
      echo "Error: checksum mismatch for git-ci" >&2
      echo "  expected $expected" >&2
      echo "  actual   $actual" >&2
      rm -f "$dest"; exit 1; }
  else
    echo "Warning: no published checksum; installing unverified" >&2
  fi
}

mkdir -p "$install_dir"
tmp="$(mktemp "${TMPDIR:-/tmp}/git-ci.XXXXXX")"
trap 'rm -f "$tmp"' EXIT

case "$source_mode" in
  local)
    [ -n "$local_script" ] || { echo "Error: no bin/git-ci beside this script" >&2; exit 1; }
    cp "$local_script" "$tmp" ;;
  remote)
    download "$tmp" ;;
  auto)
    if [ -n "$local_script" ]; then cp "$local_script" "$tmp"; else download "$tmp"; fi ;;
  *)
    echo "Error: CI_INSTALL_SOURCE must be local, remote or auto" >&2; exit 1 ;;
esac

was="$(version_of "$target")"
install -m 0755 "$tmp" "$target"
now="$(version_of "$target")"

echo "Installed git-ci $now (was $was) to $target"

case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) echo
     echo "$install_dir is not on your PATH. Add it, then \`git ci\` will work:"
     echo "  export PATH=\"$install_dir:\$PATH\"" ;;
esac

cat <<'EOF'

Point it at your CI and give it the signing secret:

  git config ci.endpoint https://ci.example.com
  git config ci.secret   <the server's CI_WEBHOOK_SECRET>

Then, from a repository with .ci/workflows/*.yml:

  git ci --dry-run    # see what would be sent
  git ci              # submit HEAD
EOF
