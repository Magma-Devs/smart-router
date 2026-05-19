#!/usr/bin/env bash
# Print one version's section from CHANGELOG.md.
#
# Usage: scripts/extract-changelog.sh v1.0.0
#
# Used by .goreleaser.yaml as a before-hook to write dist/release-notes.md,
# which goreleaser then uses as the GitHub release body.
#
# Env:
#   IS_SNAPSHOT  optional - set to 1 to emit a placeholder + exit 0 instead
#                of looking up a CHANGELOG.md section. `make snapshot` sets
#                this so snapshot builds (which use synthesized tags that
#                won't match real CHANGELOG sections) don't fail the hook.

set -euo pipefail

if [ "${IS_SNAPSHOT:-0}" = "1" ]; then
  printf '*Snapshot build - no CHANGELOG.md section.*\n'
  exit 0
fi

version="${1:?usage: extract-changelog.sh <version>}"

cd "$(git rev-parse --show-toplevel)"

if [ ! -f CHANGELOG.md ]; then
  echo "extract-changelog: CHANGELOG.md not found" >&2
  exit 1
fi

# Escape regex metacharacters in the version string for the awk pattern.
v_re="$(printf '%s' "$version" | sed -e 's/[][().*+?^$\|/]/\\&/g')"

out="$(awk -v v="$v_re" '
  $0 ~ "^## " v "( |$)" { capture = 1; print; next }
  capture && /^## /     { exit }
  capture                { print }
' CHANGELOG.md)"

if [ -z "$out" ]; then
  echo "extract-changelog: no '## $version' section in CHANGELOG.md" >&2
  exit 1
fi

printf '%s\n' "$out"
