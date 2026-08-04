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
// default — secondary options without an address are a deployment mistake, not a
// configuration. primaryAddress feeds the two advisory warnings: secondary-only is
// explicitly allowed but warned about (no backfill), and secondary == primary
// is legal but almost certainly unintended.
func (c SecondaryCacheConfig) Validate(primaryAddress string, timeoutSet, modeSet bool) (warnings []string, err error) {
	if !c.Enabled() {
		if timeoutSet || modeSet {
			return nil, fmt.Errorf("secondary cache options are set while %s is empty — dangling configuration (set %s or drop %s/%s)",
				SecondaryCacheFlagName, SecondaryCacheFlagName, SecondaryCacheTimeoutFlagName, SecondaryCacheModeFlagName)
		}
		return nil, nil
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
