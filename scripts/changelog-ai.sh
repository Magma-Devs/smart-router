#!/usr/bin/env bash
# Call Gemini Flash with a prompt on stdin, print the response on stdout.
#
# Used by changelog-bump.sh to draft the Highlights section. Caller treats
# a non-zero exit as "fall back to TODO placeholder".
#
# Env:
#   GEMINI_API_KEY  required  - Google AI Studio key (https://aistudio.google.com/apikey)
#   GEMINI_MODEL    optional  - default: gemini-pro-latest (best-quality
#                               Pro model alias; requires paid billing).
#                               Override with gemini-2.5-flash for a free-
#                               tier-compatible fallback. See model block
#                               below for the full reasoning.

set -euo pipefail

: "${GEMINI_API_KEY:?GEMINI_API_KEY not set}"
# `gemini-pro-latest` is the stable alias to Google's top-tier Pro model
# (currently 3.1-pro). Higher-quality prose than Flash for release-notes
# Highlights, at the cost of higher latency and paid-tier billing —
# Google does not offer free-tier quota on Pro models. If the key set
# as GEMINI_API_KEY isn't enrolled in a paid plan, calls return 429
# and the bumper falls back to a TODO placeholder (release still ships,
# the dev edits highlights manually before publishing the draft).
#
# Override with GEMINI_MODEL=gemini-2.5-flash for free-tier-compatible
# drafts (lower quality but no billing required).
GEMINI_MODEL="${GEMINI_MODEL:-gemini-pro-latest}"

prompt="$(cat)"
if [ -z "$prompt" ]; then
  echo "changelog-ai: empty prompt on stdin" >&2
  exit 1
fi

response="$(
  curl -sS --fail-with-body \
    "https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}" \
    -H "Content-Type: application/json" \
    -d "$(jq -n --arg p "$prompt" '{
          contents: [{parts: [{text: $p}]}],
          generationConfig: {temperature: 0.4, maxOutputTokens: 8192}
        }')"
)" || {
  echo "changelog-ai: Gemini API call failed:" >&2
  echo "$response" >&2
  exit 1
}

if echo "$response" | jq -e '.error' >/dev/null 2>&1; then
  echo "changelog-ai: Gemini returned an error:" >&2
  echo "$response" | jq -r '.error.message // .error' >&2
  exit 1
fi

text="$(echo "$response" | jq -r '.candidates[0].content.parts[0].text // empty')"
if [ -z "$text" ] || [ "$text" = "null" ]; then
  echo "changelog-ai: Gemini returned no text (likely safety-filtered or empty response)" >&2
  echo "$response" | jq '.' >&2
  exit 1
fi

printf '%s\n' "$text"
