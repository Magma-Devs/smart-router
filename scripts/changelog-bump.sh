#!/usr/bin/env bash
# Prepend a new release section to CHANGELOG.md.
#
# Groups commits since the most recent v*.*.* tag (or the fork-base commit
# if no prior tag) by conventional-commit prefix, emits a new ## v<version>
# section with:
#   - ### Highlights  (Gemini draft, then $EDITOR for human review)
#   - ### Changes     (grouped commit bullets, each tagged with [hash])
#   - reference-link blob mapping [hash] -> commit URL
#
# Usage:
#   VERSION=v1.0.0 scripts/changelog-bump.sh
#   AI=0 VERSION=v1.0.0 scripts/changelog-bump.sh          # skip Gemini draft
#   EDIT=0 VERSION=v1.0.0 scripts/changelog-bump.sh        # skip $EDITOR (CI / dry-run)
#
# Env:
#   VERSION         required  - e.g. v1.0.0
#   AI              optional  - 0 to skip Gemini draft (default 1)
#   EDIT            optional  - 0 to skip $EDITOR (default 1)
#   EDITOR          optional  - defaults to vim
#   GEMINI_API_KEY  optional  - passed through to changelog-ai.sh
#   REPO_URL        optional  - default: https://github.com/magma-Devs/smart-router

set -euo pipefail

# Smart-router initial-import commit. Used as the "since" marker for the
# first release after v0.0.0-alpha is deleted, so the changelog does not
# walk into the imported lavanet/lava upstream history.
FORK_BASE="10a450617b7ee6d1aff724030f3a55b60a694533"

REPO_URL="${REPO_URL:-https://github.com/magma-Devs/smart-router}"
EDITOR="${EDITOR:-vim}"

: "${VERSION:?VERSION is required, e.g. VERSION=v1.0.0}"

if ! [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$ ]]; then
  echo "VERSION must look like v1.0.0 or v1.0.0-rc1 (got: $VERSION)" >&2
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"
script_dir="$(dirname "$(readlink -f "$0")")"

# Refuse to clobber an existing section. The release workflow detects
# this and skips the bumper entirely on re-runs; if the user is hitting
# this locally, they should either bump VERSION or hand-edit CHANGELOG.md.
v_re_escaped="$(printf '%s' "$VERSION" | sed -e 's/[][().*+?^$\|/]/\\&/g')"
if [ -f CHANGELOG.md ] && grep -qE "^## ${v_re_escaped}( |\$)" CHANGELOG.md; then
  echo "changelog-bump: CHANGELOG.md already has a '## ${VERSION}' section - refusing to add a duplicate" >&2
  echo "changelog-bump: edit the existing section by hand, or remove it first" >&2
  exit 1
fi

prev_ref="$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' HEAD 2>/dev/null || true)"
if [ -z "$prev_ref" ]; then
  prev_ref="$FORK_BASE"
  echo "changelog-bump: no prior v*.*.* tag - using fork base ${FORK_BASE:0:8}" >&2
else
  echo "changelog-bump: prior tag $prev_ref" >&2
fi

# Conventional-commit grouping. Order matters: first match wins. Buckets
# mirror what .goreleaser.yaml's changelog.groups config used to do, so
# the per-commit breakdown stays consistent with prior expectations.
GROUP_TITLES=(
  "Dependency updates"
  "New Features"
  "Security updates"
  "Bug fixes"
  "Documentation updates"
  "Build process updates"
  "Other work"
)
GROUP_REGEX=(
  '^(feat|fix|chore)\(deps\)!?:'
  '^feat(\(.+\))?!?:'
  '^sec(\(.+\))?!?:'
  '^(fix|refactor)(\(.+\))?!?:'
  '^docs?(\(.+\))?!?:'
  '^(build|ci)(\(.+\))?!?:'
  '.'
)
EXCLUDE_REGEX='^(test:|test\(|chore:|chore\(|Merge (pull request|remote-tracking branch|branch)|go mod tidy)'

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
for i in "${!GROUP_TITLES[@]}"; do : > "$tmp_dir/g$i"; done
: > "$tmp_dir/refs"

# `git log --reverse` yields oldest-first inside the range; the inline
# bullet list ends up in chronological order, matching the old goreleaser
# `sort: asc` behavior.
while IFS=$'\t' read -r short long subject; do
  if [[ "$subject" =~ $EXCLUDE_REGEX ]]; then
    continue
  fi
  for i in "${!GROUP_REGEX[@]}"; do
    if [[ "$subject" =~ ${GROUP_REGEX[$i]} ]]; then
      printf -- '- %s [`%s`]\n' "$subject" "$short" >> "$tmp_dir/g$i"
      printf '[`%s`]: %s/commit/%s\n' "$short" "$REPO_URL" "$long" >> "$tmp_dir/refs"
      break
    fi
  done
done < <(git log --reverse --no-merges --pretty=format:'%h%x09%H%x09%s' "${prev_ref}..HEAD")

changes_md=""
for i in "${!GROUP_TITLES[@]}"; do
  if [ -s "$tmp_dir/g$i" ]; then
    changes_md+=$'\n'"#### ${GROUP_TITLES[$i]}"$'\n'
    changes_md+="$(cat "$tmp_dir/g$i")"$'\n'
  fi
done

if [ -z "$changes_md" ]; then
  echo "changelog-bump: no commits in ${prev_ref}..HEAD after filtering - nothing to release" >&2
  exit 1
fi

highlights="TODO: write a 2-4 sentence Highlights paragraph for this release."

if [ "${AI:-1}" != "0" ] && [ -n "${GEMINI_API_KEY:-}" ]; then
  prior_highlights=""
  if [ -f CHANGELOG.md ]; then
    prior_highlights="$(awk '
      /^### Highlights/ { capture = 1; next }
      /^### Changes/    { if (capture) exit }
      capture            { print }
    ' CHANGELOG.md | sed -e '/./,$!d' | head -c 2000 || true)"
  fi

  grouped_for_prompt=""
  for i in "${!GROUP_TITLES[@]}"; do
    if [ -s "$tmp_dir/g$i" ]; then
      grouped_for_prompt+=$'\n'"## ${GROUP_TITLES[$i]}"$'\n'
      grouped_for_prompt+="$(cat "$tmp_dir/g$i")"$'\n'
    fi
  done

  prompt="$(cat <<EOF
You are drafting the Highlights paragraph for smart-router release ${VERSION}.

Smart Router is a multi-protocol RPC gateway (JSON-RPC, REST, gRPC, Tendermint)
with QoS-scored upstream routing, finalization-aware caching, and provider
failover. Audience: integrators and SREs running the router in production.

Style: factual, terse, engineering tone. No marketing words like "powerful",
"enhanced", "seamless", "robust", "leverage", "ecosystem". Lead with what is
customer-visible. Skip CI/build/release-plumbing changes unless they directly
affect users (e.g. signed checksums, new platform support).

Prior release Highlights (match this voice; do not copy):
${prior_highlights:-<this is the first release with a Highlights section - no prior reference>}

Commits in this release, grouped by type:
${grouped_for_prompt}

Output: 2 to 4 sentences of plain prose. No header, no preamble, no bullet list.
Plain text only - the result will be inserted verbatim under a "### Highlights"
markdown heading.
EOF
)"

  if drafted="$(echo "$prompt" | "${script_dir}/changelog-ai.sh")"; then
    drafted="$(echo "$drafted" | sed -e 's/[[:space:]]*$//' -e '/^$/d')"
    if [ -n "$drafted" ]; then
      highlights="$drafted"
      echo "changelog-bump: Gemini drafted Highlights ($(echo "$drafted" | wc -w | tr -d ' ') words)" >&2
    fi
  else
    echo "changelog-bump: Gemini draft failed - using TODO placeholder" >&2
  fi
elif [ "${AI:-1}" != "0" ]; then
  echo "" >&2
  echo "  ============================================================" >&2
  echo "  WARNING: GEMINI_API_KEY not set" >&2
  echo "  The Highlights section will be a TODO placeholder you must" >&2
  echo "  write yourself. Get a key from https://aistudio.google.com/apikey" >&2
  echo "  or re-run with AI=0 to acknowledge this explicitly." >&2
  echo "  ============================================================" >&2
  echo "" >&2
else
  echo "changelog-bump: AI=0 - Highlights will be a TODO placeholder" >&2
fi

date_iso="$(date -u +%Y-%m-%d)"

new_section="$(cat <<EOF
## ${VERSION} — ${date_iso}

### Highlights

${highlights}

### Changes
${changes_md}
$(cat "$tmp_dir/refs")
EOF
)"

if [ ! -f CHANGELOG.md ]; then
  printf '# Changelog\n\n%s\n' "$new_section" > CHANGELOG.md
  echo "changelog-bump: created CHANGELOG.md" >&2
else
  if ! head -1 CHANGELOG.md | grep -q '^# Changelog'; then
    echo "changelog-bump: CHANGELOG.md does not start with '# Changelog' - refusing to clobber" >&2
    exit 1
  fi
  # Insert new section just before the first existing ## heading. If no
  # ## heading exists (fresh scaffold), append at the end.
  awk -v ns="$new_section" '
    BEGIN { inserted = 0 }
    /^## / && !inserted { print ns; print ""; inserted = 1 }
    { print }
    END { if (!inserted) { print ""; print ns } }
  ' CHANGELOG.md > CHANGELOG.md.tmp
  mv CHANGELOG.md.tmp CHANGELOG.md
  echo "changelog-bump: prepended ${VERSION} section to CHANGELOG.md" >&2
fi

if [ "${EDIT:-1}" = "0" ]; then
  echo "changelog-bump: EDIT=0, skipping \$EDITOR" >&2
  exit 0
fi

echo "changelog-bump: opening \$EDITOR (${EDITOR}) for Highlights review..." >&2

case "$(basename "$EDITOR")" in
  vim|nvim|vi)
    "$EDITOR" -c "/^### Highlights/+2" CHANGELOG.md
    ;;
  nano)
    line="$(grep -n '^### Highlights' CHANGELOG.md | head -1 | cut -d: -f1 || echo 1)"
    "$EDITOR" "+$((line + 2))" CHANGELOG.md
    ;;
  *)
    "$EDITOR" CHANGELOG.md
    ;;
esac
