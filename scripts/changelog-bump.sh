#!/usr/bin/env bash
# Prepend a new release section to CHANGELOG.md.
#
# Groups commits since the most recent v*.*.* tag (or the fork-base commit
# if no prior tag) by conventional-commit prefix, emits a new ## v<version>
# section with:
#   - ### Highlights  (Gemini draft, then $EDITOR for human review)
#   - ### Changes     (grouped commit bullets, each tagged with [hash])
#       leads with a "#### ⚠ Breaking changes" group when any commit is
#       breaking; the same commits also appear under their normal group
#   - reference-link blob mapping [hash] -> commit URL
#
# Breaking changes are detected from BOTH conventional-commit conventions:
# the `!` marker in the subject (feat!:, chore(x)!:) and a BREAKING CHANGE:
# footer in the commit body. The footer's migration note is surfaced as a
# sub-bullet, and the breaking set is fed to Gemini as its own block with
# an instruction to lead the Highlights with it.
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
#   FORCE           optional  - 1 to REPLACE an existing '## <version>'
#                               section instead of refusing (default 0).
#                               Use when a tag was deleted + recreated from
#                               scratch and the stale section must be
#                               regenerated from the new commit range.
#   EDITOR          optional  - defaults to vim
#   GEMINI_API_KEY  optional  - passed through to changelog-ai.sh
#   REPO_URL        optional  - default: https://github.com/magma-Devs/smart-router

set -euo pipefail

# Smart-router initial-import commit. Used as the "since" marker for the
# first release after v0.0.0-alpha is deleted, so the changelog does not
# walk into the imported lavanet/lava upstream history.
FORK_BASE="10a450617b7ee6d1aff724030f3a55b60a694533"

REPO_URL="${REPO_URL:-https://github.com/magma-Devs/smart-router}"
# Owner/repo path derived from REPO_URL — used for GitHub API calls
# that look up the PR associated with each commit (see PR-link logic
# in the per-commit loop below).
REPO_PATH="${REPO_URL#https://github.com/}"
EDITOR="${EDITOR:-vim}"

: "${VERSION:?VERSION is required, e.g. VERSION=v1.0.0}"

if ! [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$ ]]; then
  echo "VERSION must look like v1.0.0 or v1.0.0-rc1 (got: $VERSION)" >&2
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"
script_dir="$(dirname "$(readlink -f "$0")")"

# An existing '## <version>' section normally means a re-run: refuse to
# add a duplicate. FORCE=1 overrides this — it strips the old section so
# the fresh one (built below) replaces it. That's the path for a tag that
# was deleted and recreated from scratch: the CHANGELOG.md on main still
# carries the stale section, and we want it regenerated from the new
# commit range, not preserved.
v_re_escaped="$(printf '%s' "$VERSION" | sed -e 's/[][().*+?^$\|/]/\\&/g')"
if [ -f CHANGELOG.md ] && grep -qE "^## ${v_re_escaped}( |\$)" CHANGELOG.md; then
  if [ "${FORCE:-0}" = "1" ]; then
    echo "changelog-bump: FORCE=1 - replacing existing '## ${VERSION}' section" >&2
    # Delete the old section: from its '## <version>' heading up to (but
    # not including) the next '## ' heading, or EOF if it's the last one.
    # We match the heading by exact string, not regex, so a version whose
    # name is a prefix of another (v1.1.0 vs v1.1.0-rc1) can't cross-match:
    # the line must be exactly "## <version>" OR "## <version> " (space +
    # date suffix). awk `index()`/substr on the raw literal avoids any
    # regex-escaping and the `\.`/`\$` warnings that came with it.
    awk -v hdr="## ${VERSION}" '
      # Start skipping at the EXACT target heading (bare, or "## <ver> <date>").
      # The exact/space-prefixed test means a longer version that merely
      # starts with this one (v1.1.0 vs v1.1.0-rc1) is not matched here.
      $0 == hdr || substr($0, 1, length(hdr) + 1) == hdr " " { skip = 1; next }
      # Any subsequent "## " heading is, by definition, a different section
      # (the target already consumed itself via next), so it ends the skip.
      skip && /^## / { skip = 0 }
      !skip { print }
    ' CHANGELOG.md > CHANGELOG.md.tmp
    mv CHANGELOG.md.tmp CHANGELOG.md
  else
    echo "changelog-bump: CHANGELOG.md already has a '## ${VERSION}' section - refusing to add a duplicate" >&2
    echo "changelog-bump: edit the existing section by hand, remove it first, or re-run with FORCE=1" >&2
    exit 1
  fi
fi

# --exclude "$VERSION" is critical when the release workflow runs on a
# freshly-pushed tag: HEAD is at the new tag, so without exclusion
# `git describe` returns the new tag itself as "prior" and `git log
# <new>..HEAD` is empty. With exclusion, we get the actual previous
# v*.*.* tag (or fall through to FORK_BASE for the first release).
prev_ref="$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' --exclude "$VERSION" HEAD 2>/dev/null || true)"
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
# Breaking changes are collected separately and rendered as a dedicated
# group at the TOP of ### Changes (and fed to Gemini as a distinct block),
# regardless of which conventional-commit bucket they'd otherwise land in.
# A commit is "breaking" if either convention is present:
#   - the `!` marker before the colon in the subject (feat!:, chore(x)!:)
#   - a `BREAKING CHANGE:` / `BREAKING-CHANGE:` footer in the commit body
: > "$tmp_dir/breaking"
BREAKING_SUBJECT_RE='^[a-z]+(\([^)]*\))?!:'

# `git log --reverse` yields oldest-first inside the range; the inline
# bullet list ends up in chronological order, matching the old goreleaser
# `sort: asc` behavior.
#
# For each commit we resolve a PR number two ways: first by looking for
# the `(#N)` suffix that GitHub appends to squash-merged commit subjects,
# and if that's missing, by calling `GET /commits/{sha}/pulls` against
# the GitHub API (auth via GH_TOKEN). Unauthenticated API calls are rate-
# limited to 60/hour, so the workflow passes GITHUB_TOKEN as GH_TOKEN.
# Without a token, we still surface any (#N) baked into the subject.
while IFS=$'\t' read -r short long subject; do
  if [[ "$subject" =~ $EXCLUDE_REGEX ]]; then
    continue
  fi

  # Detect a breaking change. The subject `!` marker is cheap to test
  # inline; the BREAKING CHANGE: footer lives in the body, so pull the
  # body for this one commit and grep it. (We can't read the body in the
  # main `git log` stream because bodies are multi-line and would break
  # the tab-delimited `read`.)
  is_breaking=0
  breaking_note=""
  if [[ "$subject" =~ $BREAKING_SUBJECT_RE ]]; then
    is_breaking=1
  fi
  body="$(git log -1 --pretty=format:'%b' "$long")"
  if printf '%s' "$body" | grep -qiE '^BREAKING[ -]CHANGE:'; then
    is_breaking=1
    # Capture the full footer paragraph (the BREAKING CHANGE: footer can
    # wrap across several lines; it ends at the next blank line or EOF) so
    # the human reader and Gemini get the whole migration note, not just
    # its first wrapped line. Strip the leading token, then collapse the
    # paragraph to a single line so it renders as one clean markdown
    # sub-bullet.
    breaking_note="$(printf '%s\n' "$body" | awk '
      /^BREAKING[ -]CHANGE:/ { c=1 }
      c && /^[[:space:]]*$/   { exit }
      c                       { print }
    ' | sed -E '1 s/^BREAKING[ -]CHANGE:[[:space:]]*//' | tr '\n' ' ' | sed -E 's/[[:space:]]+/ /g; s/[[:space:]]+$//')"
  fi

  pr_num=""
  subject_rendered="$subject"
  if [[ "$subject" =~ \(#([0-9]+)\)[[:space:]]*$ ]]; then
    pr_num="${BASH_REMATCH[1]}"
    # Replace the trailing `(#N)` with `([#N])` so it renders as a
    # markdown reference-style link (definition added to $tmp_dir/refs
    # below; deduped via sort -u at end of section build).
    subject_rendered="${subject%\(#${pr_num}\)*}([#${pr_num}])"
  elif [ -n "${GH_TOKEN:-}" ]; then
    pr_num="$(curl -sS --max-time 5 \
      -H "Authorization: token ${GH_TOKEN}" \
      -H "Accept: application/vnd.github+json" \
      "https://api.github.com/repos/${REPO_PATH}/commits/${long}/pulls" 2>/dev/null \
      | jq -r '.[0].number // empty' 2>/dev/null || true)"
    if [ -n "$pr_num" ]; then
      subject_rendered="${subject} ([#${pr_num}])"
    fi
  fi

  if [ "$is_breaking" = "1" ]; then
    # Append the migration note inline when the footer gave us one.
    if [ -n "$breaking_note" ]; then
      printf -- '- %s [`%s`]\n  - %s\n' "$subject_rendered" "$short" "$breaking_note" >> "$tmp_dir/breaking"
    else
      printf -- '- %s [`%s`]\n' "$subject_rendered" "$short" >> "$tmp_dir/breaking"
    fi
    printf '[`%s`]: %s/commit/%s\n' "$short" "$REPO_URL" "$long" >> "$tmp_dir/refs"
    if [ -n "$pr_num" ]; then
      printf '[#%s]: %s/pull/%s\n' "$pr_num" "$REPO_URL" "$pr_num" >> "$tmp_dir/refs"
    fi
  fi

  for i in "${!GROUP_REGEX[@]}"; do
    if [[ "$subject" =~ ${GROUP_REGEX[$i]} ]]; then
      printf -- '- %s [`%s`]\n' "$subject_rendered" "$short" >> "$tmp_dir/g$i"
      printf '[`%s`]: %s/commit/%s\n' "$short" "$REPO_URL" "$long" >> "$tmp_dir/refs"
      if [ -n "$pr_num" ]; then
        printf '[#%s]: %s/pull/%s\n' "$pr_num" "$REPO_URL" "$pr_num" >> "$tmp_dir/refs"
      fi
      break
    fi
  done
done < <(git log --reverse --no-merges --pretty=tformat:'%h%x09%H%x09%s' "${prev_ref}..HEAD")
# tformat: (terminator) appends a newline after every commit; plain
# format: only puts newlines BETWEEN commits, so bash `read` drops the
# last line in the loop above. With tformat: every commit is processed.

# Dedup ref lines: a single PR can be linked from multiple commits.
sort -u -o "$tmp_dir/refs" "$tmp_dir/refs"

changes_md=""
# Breaking changes lead the section so a reader scanning the changelog
# sees what will break before the routine feature/fix bullets. The same
# commits still also appear under their normal conventional-commit group.
if [ -s "$tmp_dir/breaking" ]; then
  changes_md+=$'\n'"#### ⚠ Breaking changes"$'\n'
  changes_md+="$(cat "$tmp_dir/breaking")"$'\n'
fi
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

  # Breaking changes are pulled out into their own block so the prompt can
  # instruct Gemini to lead with them. Empty when the release has none.
  breaking_for_prompt=""
  if [ -s "$tmp_dir/breaking" ]; then
    breaking_for_prompt="$(cat "$tmp_dir/breaking")"
  fi

  prompt="$(cat <<EOF
You are drafting the Highlights paragraph for smart-router release ${VERSION}.

Smart Router is a multi-protocol RPC gateway (JSON-RPC, REST, gRPC, Tendermint)
with QoS-scored upstream routing, finalization-aware caching, and provider
failover. Audience: integrators and SREs running the router in production.

Write a single coherent paragraph (4-6 sentences) that describes what
this release brings — flowing prose, NOT a list of features one after
another. Weave the most important customer-visible changes together so
the reader gets the gist of the release as a whole, not a roll call of
commits. Name concrete artifacts (commands, endpoints, flags, headers,
env vars, file names) inline when they fit the sentence naturally;
don't shoehorn every feature in.

BREAKING CHANGES — this is the most important rule. The commits listed
under "Breaking changes" below remove, rename, or change the behavior of
something integrators depend on; upgrading without action will break a
working deployment. You MUST call out every breaking change explicitly
and FIRST, before describing new features. State plainly what was removed
or changed and what the operator must do (e.g. "the X flag is gone —
move to Y", "config key Z is no longer read"). Name the exact flag /
config key / endpoint / env var. Do not soften, bury, or omit a breaking
change. If the "Breaking changes" block below is empty, say nothing about
breaking changes and do not claim the release is backward-compatible.

Style: factual, engineering tone, specific. Banned marketing words:
"powerful", "enhanced", "seamless", "robust", "leverage", "ecosystem",
"comprehensive", "unlock", "delight". Skip pure CI / build-plumbing
changes unless they directly affect users (signed checksums, new
platform support).

Prior release Highlights (match this voice; do not copy):
${prior_highlights:-<this is the first release with a Highlights section - no prior reference>}

Breaking changes in this release (lead the paragraph with these; empty = none):
${breaking_for_prompt:-<none - this release has no breaking changes>}

Commits in this release, grouped by type:
${grouped_for_prompt}

Output: plain prose only — no headers, no bullets, no preamble, no
sentence-per-bullet structure. The result is inserted verbatim under a
"### Highlights" markdown heading.
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
