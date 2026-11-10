package metrics

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/prometheus/client_golang/prometheus"
)

// goPseudoVersion matches the version the Go toolchain stamps on a build made after the release
// tag vX.Y.Z when -ldflags sets none (see version/version.go): vX.Y.(Z+1)-0.<UTC time>-<commit>,
// with "+dirty" for a modified tree. Its X.Y.(Z+1) names a release that does not exist yet, so the
// gauge reports X.Y.Z, as it does for git describe's vX.Y.Z-N-g<commit>.
var goPseudoVersion = regexp.MustCompile(`^v?\d+\.\d+\.\d+-0\.\d{14}-[0-9a-f]{12}(\+[0-9A-Za-z.-]+)?$`)

// SetVersionInner encodes a semantic version string onto a protocol-version gauge as
// major*1e6 + minor*1e3 + patch, so dashboards can compare deployed versions numerically.
// Used by SmartRouterMetricsManager to set its protocol_version gauge.
func SetVersionInner(protocolVersionMetric *prometheus.GaugeVec, version string) {
	// Normalize git-describe style: strip leading "v" and drop everything from the first "-" or "+".
	// Examples: "v6.2.2" → "6.2.2", "v6.2.2-3-gabc1234-dirty" → "6.2.2".
	cleaned := strings.TrimPrefix(version, "v")
	if i := strings.IndexAny(cleaned, "-+"); i >= 0 {
		cleaned = cleaned[:i]
	}
	var major, minor, patch int
	_, err := fmt.Sscanf(cleaned, "%d.%d.%d", &major, &minor, &patch)
	if err != nil {
		utils.LavaFormatError("Failed parsing version at metrics manager", err, utils.LogAttr("version", version))
		protocolVersionMetric.WithLabelValues("version").Set(0)
		return
	}
	if patch > 0 && goPseudoVersion.MatchString(version) {
		patch--
	}
	combined := major*1000000 + minor*1000 + patch
	protocolVersionMetric.WithLabelValues("version").Set(float64(combined))
}
