package performance

import (
	"fmt"
	"time"
)

// SecondaryCacheConfig carries the operator-provided secondary cache settings
// (docs/SECONDARY-CACHE.md).
type SecondaryCacheConfig struct {
	Address string
	Timeout time.Duration
	Mode    string
}

// Enabled reports whether a secondary cache is configured at all.
func (c SecondaryCacheConfig) Enabled() bool {
	return c.Address != ""
}

// Validate applies the secondary-cache startup rules (docs/SECONDARY-CACHE.md).
// A hard error fails startup; warnings are logged and startup proceeds.
// timeoutSet/modeSet distinguish an explicitly provided flag/YAML value from the
// default. primaryAddress feeds the two advisory warnings: secondary-only is
// explicitly allowed but warned about (no backfill), and secondary == primary
// is legal but almost certainly unintended.
//
// Tuning options with no address are reported as a warning rather than a startup
// failure. They are usually a typo worth surfacing, but the same shape occurs
// legitimately when one templated YAML is shared across a fleet in which only some
// routers run a secondary — and failing startup there turns a harmless unused key
// into an outage on every router that does not. The warning still names the problem,
// and the secondary is simply left disabled.
func (c SecondaryCacheConfig) Validate(primaryAddress string, timeoutSet, modeSet bool) (warnings []string, err error) {
	if !c.Enabled() {
		if timeoutSet || modeSet {
			warnings = append(warnings, fmt.Sprintf("secondary cache options (%s/%s) are set while %s is empty — dangling configuration, secondary cache disabled (set %s to enable it, or drop the unused options)",
				SecondaryCacheTimeoutFlagName, SecondaryCacheModeFlagName, SecondaryCacheFlagName, SecondaryCacheFlagName))
		}
		return warnings, nil
	}
	if c.Timeout <= 0 {
		return nil, fmt.Errorf("%s must be greater than zero, got %s", SecondaryCacheTimeoutFlagName, c.Timeout)
	}
	if c.Mode != SecondaryCacheModeReadOnly {
		return nil, fmt.Errorf("%s must be %q (read-write is reserved for a future iteration), got %q",
			SecondaryCacheModeFlagName, SecondaryCacheModeReadOnly, c.Mode)
	}
	if primaryAddress == "" {
		warnings = append(warnings, "secondary cache configured without a primary ("+CacheFlagName+" empty): reads work but nothing backfills, so repeat requests re-query the secondary")
	} else if c.Address == primaryAddress {
		warnings = append(warnings, "secondary cache address equals the primary ("+c.Address+"): every miss double-queries the same store, almost certainly a misconfiguration")
	}
	return warnings, nil
}
