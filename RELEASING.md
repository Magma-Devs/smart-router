# Releasing smart-router

Releasing is automatic on tag push. The workflow generates the
`CHANGELOG.md` entry via Gemini Flash, commits it back to `main`, builds
binaries + multi-arch Docker image + cosign-signed checksums, and
publishes a **draft** GitHub release. Your only manual step is reviewing
the draft and clicking **Publish**.

## Cutting a release

1. Make sure `main` is at the commit you want to ship.

2. Tag and push:

   ```
   git tag v1.0.0
   git push origin v1.0.0
   ```

3. The release workflow (`.github/workflows/release.yml`) runs:

   - Calls `scripts/changelog-bump.sh` with `secrets.GEMINI_API_KEY`.
     The bumper groups commits since the previous `v*.*.*` tag by
     conventional-commit prefix (`feat:`, `fix:`, `docs:`, `build:`/`ci:`,
     deps, security, other), asks Gemini Flash to draft a 2–4 sentence
     Highlights paragraph, and prepends the new `## v1.0.0 — <date>`
     section to `CHANGELOG.md`.
   - Commits the `CHANGELOG.md` update to `main` with `[skip ci]` so the
     workflow doesn't re-trigger itself. The commit is a fast-forward —
     if `main` has moved past the tag, the push fails and the workflow
     stops (manual intervention required).
   - Extracts the just-generated section to `.release-notes/body.md`.
   - Runs GoReleaser: builds 4 static binaries, multi-arch Docker image
     (pushed to GHCR), `sha256sum.txt`, and cosign-keyless signs the
     checksum file via GitHub Actions OIDC.
   - Publishes a **draft** GitHub release with the extracted section as
     the body.

4. Open the draft on github.com. Read the Highlights paragraph. If it
   reads well, click **Publish**. If not, edit the body in the GitHub
   UI before publishing.

   **Edits to the body on github.com do NOT sync back to `CHANGELOG.md`
   in `main`.** If you make significant edits, open a follow-up PR to
   update `CHANGELOG.md`, or accept that the file and the release page
   may diverge slightly for that version.

## Required setup

### Repository secret

Set `GEMINI_API_KEY` as a repository (or organization) secret:

```
gh secret set GEMINI_API_KEY --body '<your-key>' --repo magma-Devs/smart-router
```

Get a key from <https://aistudio.google.com/apikey>. The free tier
(15 RPM, 1M tokens/day) covers any reasonable release cadence.

If the secret is missing, the bumper falls back to a `TODO: write
Highlights` placeholder and the workflow still publishes — but the
draft release will have a visible TODO that you'll need to edit before
publishing.

### Branch protection on `main`

The workflow pushes the `CHANGELOG.md` commit to `main` using the
default `GITHUB_TOKEN`. If `main` is branch-protected with "require PR
review" or "require status checks", that push will be rejected.

Two ways to fix:

- **Allow bypass for `github-actions[bot]`** in the `main` branch
  protection rules (cleanest, no extra credentials).
- **Use a fine-grained PAT** with `contents: write` on this repo, store
  it as `secrets.RELEASE_TOKEN`, and swap `GITHUB_TOKEN` for it in the
  commit step.

If neither is set up, the release still publishes — but `CHANGELOG.md`
in `main` won't be updated, and you'll need to commit it manually
afterwards.

## Manually previewing a changelog locally

You don't need this for a normal release — CI does the same thing on
tag push. But if you want to preview what the workflow will generate,
or hand-write a special-case entry before tagging, run:

```
GEMINI_API_KEY=<...> make changelog VERSION=v1.0.0
```

**`GEMINI_API_KEY` is required to get the Highlights paragraph.** The
script reads it from the environment (the repo secret is only visible
inside GitHub Actions — locally you need your own key from
<https://aistudio.google.com/apikey>). With the key set, the bumper
drafts the Highlights via Gemini Flash and opens `$EDITOR` for review.

If you commit the resulting `CHANGELOG.md` change before pushing the
tag, the workflow detects the pre-existing section on tag push and
skips its own bumper run — your hand-edited Highlights ship as-is.

### Running without a key (no Highlights)

If you don't have a Gemini key or want to skip the LLM step explicitly:

```
AI=0 make changelog VERSION=v1.0.0
```

The bumper still groups the commits into a Changes section, but the
Highlights area is filled with `TODO: write a 2-4 sentence Highlights
paragraph for this release.` You're expected to write the paragraph
yourself in `$EDITOR` before saving. If you save the file with the TODO
intact, the TODO ships verbatim in the release body — you'll see it
when reviewing the draft and need to edit before publishing.

The same TODO fallback fires automatically if you forget to set
`GEMINI_API_KEY` while leaving `AI=1` (default) — the bumper warns on
stderr and proceeds with the placeholder.

Other variants:

```
EDIT=0 make changelog VERSION=v1.0.0   # don't open $EDITOR (CI / dry-run)
```

## Release artifacts

Each release publishes:

- `smartrouter-v<X.Y.Z>-{linux,darwin}-{amd64,arm64}` — static binaries
- `ghcr.io/magma-devs/smart-router:v<X.Y.Z>` — multi-arch Docker image
  (linux/amd64, linux/arm64). The `:latest` tag is updated only for
  non-prerelease versions.
- `sha256sum.txt` — SHA-256 checksums of all binaries
- `sha256sum.txt.sig` + `sha256sum.txt.pem` — cosign-keyless signature
  and certificate

## Verifying the release

Install [cosign](https://docs.sigstore.dev/cosign/installation/) and run:

```
cosign verify-blob \
  --certificate sha256sum.txt.pem \
  --signature sha256sum.txt.sig \
  --certificate-identity-regexp 'https://github.com/magma-Devs/smart-router/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  sha256sum.txt
```

A successful verify means the checksum file was produced by the release
workflow on the `magma-Devs/smart-router` repo. Then verify your binary:

```
grep smartrouter-v1.0.0-linux-amd64 sha256sum.txt | sha256sum -c
```

## Versioning

- `vX.Y.Z` (no suffix) → final release. Image gets `:latest` tag.
- `vX.Y.Z-rc1`, `vX.Y.Z-beta.2`, `vX.Y.Z-alpha`, etc. → prerelease.
  Goreleaser's `prerelease: auto` flags it. No `:latest` update.

Semver bump rules for smart-router are documented in `CODING_GUIDELINES.md`.

## Quirks worth knowing

- **The tag and the CHANGELOG commit are separate commits.** The tag
  is at commit X (whatever you tagged). The CHANGELOG entry for that
  release lands as commit X+1 on `main`. `git show v1.0.0` won't
  contain its own CHANGELOG section. This matches the pattern used by
  release-please and semantic-release; most consumers read CHANGELOG.md
  from `main`, not from individual tag refs.
- **Workflow re-runs are idempotent.** If you re-trigger the workflow
  for an existing tag (e.g. via `workflow_dispatch`), the bumper
  detects the existing `## vX.Y.Z` section and skips itself, so the
  CHANGELOG isn't duplicated. The release body is re-extracted and
  re-published.
- **AI failure is non-fatal.** If Gemini rate-limits or errors, the
  bumper writes the TODO placeholder and proceeds. The draft release
  ships with the placeholder visible — you'll see it when you review,
  edit accordingly.
