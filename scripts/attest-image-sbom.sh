#!/usr/bin/env bash
# Attach a CycloneDX SBOM attestation to a pushed container image with cosign.
#
# Called by goreleaser's `docker_signs:` block (see .goreleaser.yaml), once
# per pushed image ref. goreleaser passes the fully-qualified image ref
# (ghcr.io/magma-devs/smart-router:<tag>) as the sole argument.
#
# For a k8s-deployed router the image — not the raw binary — is the artifact
# operators actually run, so it carries its own SBOM attestation in the
# registry. We syft the pushed image to a CycloneDX predicate, then
# `cosign attest --type cyclonedx` uploads it as an in-toto attestation
# alongside the image. Keyless: cosign exchanges the GitHub Actions OIDC
# token with Sigstore Fulcio for a short-lived cert — same flow as the
# checksum signing in `signs:`. No private key to manage.
#
# Verify with:
#   cosign verify-attestation --type cyclonedx \
#     --certificate-identity-regexp \
#       '^https://github.com/Magma-Devs/smart-router/.github/workflows/release.yml@refs/tags/' \
#     --certificate-oidc-issuer https://token.actions.githubusercontent.com \
#     ghcr.io/magma-devs/smart-router:<tag>
#
# Usage:   scripts/attest-image-sbom.sh <image-ref>
# Requires: syft, cosign (both installed by the release workflow).
set -euo pipefail

IMAGE="${1:?usage: attest-image-sbom.sh <image-ref>}"

for bin in syft cosign; do
  command -v "$bin" >/dev/null || { echo "missing required tool: $bin" >&2; exit 1; }
done

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
sbom="${workdir}/image.sbom.json"

echo "==> generating CycloneDX SBOM for image $IMAGE"
# syft resolves the multi-arch manifest and catalogs the image's contents.
syft "$IMAGE" --output "cyclonedx-json=$sbom"

echo "==> attesting SBOM to $IMAGE (cosign keyless)"
# --yes skips the interactive Fulcio consent prompt (non-interactive CI).
# --replace so re-running a release overwrites rather than stacking dupes.
COSIGN_EXPERIMENTAL=1 cosign attest \
  --predicate "$sbom" \
  --type cyclonedx \
  --replace \
  --yes \
  "$IMAGE"

echo "==> done. CycloneDX SBOM attestation pushed for $IMAGE"
