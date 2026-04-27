//go:build enterprise

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
