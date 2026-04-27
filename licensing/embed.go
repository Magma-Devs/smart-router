//go:build enterprise

// Embedded license loader — INTERIM design.
//
// This file (and embedded_license.txt) implements the simplest possible
// licensing path: a single shared "magma-enterprise" license is committed
// to the public repo and baked into every enterprise binary. It works while
// the public repo is private and we have one shared license for all
// customers. It does not scale to per-customer licensing or revocation, and
// it stops being meaningful if the public repo ever goes public.
//
// Sprint 6 (see docs/smart-router-repo-enterprise.md §7) replaces this with
// runtime file-loading via licensing.LoadFromFile / LoadFromEnvOrFile. When
// that sprint lands, this whole file plus embedded_license.txt is deleted.
//
// Until then: rotate the license by re-issuing in the internal repo,
// replacing embedded_license.txt, and rebuilding.
package licensing

import (
	_ "embed"
	"strings"
)

//go:embed embedded_license.txt
var embeddedLicenseEnvelope string

// EmbeddedLicense returns the enterprise license envelope baked into this
// binary at build time via go:embed. Always non-empty in builds tagged
// "enterprise" — the build fails if embedded_license.txt is missing.
//
// Callers pass this string to Validate to obtain a *License + LicenseStatus.
// The startup path (cmd/smartrouter, enterprise variant) treats the result:
//   - LicenseStatusValid       → start normally
//   - LicenseStatusGracePeriod → start with WARN log
//   - LicenseStatusExpired or error → fatal exit
func EmbeddedLicense() string {
	return strings.TrimSpace(embeddedLicenseEnvelope)
}
