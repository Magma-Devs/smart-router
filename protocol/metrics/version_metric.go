package metrics

import (
	"fmt"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/prometheus/client_golang/prometheus"
)

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
	combined := major*1000000 + minor*1000 + patch
	protocolVersionMetric.WithLabelValues("version").Set(float64(combined))
}
