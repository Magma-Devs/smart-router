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
	// Engine is CacheEngineGRPC for secondary-cache-be (also when empty) and
	// CacheEngineRESP for the secondary-resp-cache block (MAG-4778).
	Engine string
	// KeyPrefix is the RESP keyspace the tier reads, the writing zone's prefix;
	// empty for a gRPC secondary.
	KeyPrefix string
}

// SecondaryCachePrimary describes the primary tier a secondary is validated
// against. The zero value is "no primary".
type SecondaryCachePrimary struct {
	Address string
	// Engine is CacheEngineGRPC or CacheEngineRESP; empty with no primary.
	Engine string
	// KeyPrefix is the RESP keyspace the primary occupies; empty for gRPC.
	KeyPrefix string
}

// Enabled reports whether a secondary cache is configured at all.
func (c SecondaryCacheConfig) Enabled() bool {
	return c.Address != ""
}

func (c SecondaryCacheConfig) engine() string {
	if c.Engine == "" {
		return CacheEngineGRPC
	}
	return c.Engine
}

// source names the configuration the secondary came from, for messages.
func (c SecondaryCacheConfig) source() string {
	if c.engine() == CacheEngineRESP {
		return SecondaryRespCacheViperKey
	}
	return SecondaryCacheFlagName
}

// Validate applies the secondary-cache startup rules (docs/SECONDARY-CACHE.md).
// A hard error fails startup; warnings are logged and startup proceeds.
// timeoutSet/modeSet distinguish an explicitly provided flag/YAML value from the
// default. primary feeds three advisory warnings: secondary-only is explicitly
// allowed but warned about (no backfill); a secondary that is the primary's own
// store is legal but almost certainly unintended — the same address, and on RESP
// the same keyspace too, since two zones may share one backend under different
// prefixes; and a secondary on a different cache engine than the primary is a
// pairing MAG-2596 does not support. That last one is a warning and not an error
// (MAG-4777): both tiers still work, and a router already deployed that way must
// not stop starting on an upgrade.
//
// Tuning options with no address are reported as a warning rather than a startup
// failure. They are usually a typo worth surfacing, but the same shape occurs
// legitimately when one templated YAML is shared across a fleet in which only some
// routers run a secondary — and failing startup there turns a harmless unused key
// into an outage on every router that does not. The warning still names the problem,
// and the secondary is simply left disabled.
func (c SecondaryCacheConfig) Validate(primary SecondaryCachePrimary, timeoutSet, modeSet bool) (warnings []string, err error) {
	if !c.Enabled() {
		if timeoutSet || modeSet {
			warnings = append(warnings, fmt.Sprintf("secondary cache options (%s/%s) are set while %s is empty — dangling configuration, secondary cache disabled (set %s, or the %s block, to enable it, or drop the unused options)",
				SecondaryCacheTimeoutFlagName, SecondaryCacheModeFlagName, SecondaryCacheFlagName, SecondaryCacheFlagName, SecondaryRespCacheViperKey))
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
	if primary.Address == "" {
		warnings = append(warnings, "secondary cache configured without a primary ("+CacheFlagName+" empty): reads work but nothing backfills, so repeat requests re-query the secondary")
	} else if c.Address == primary.Address && c.engine() == primary.Engine && c.KeyPrefix == primary.KeyPrefix {
		warnings = append(warnings, "secondary cache address equals the primary ("+c.Address+"): every miss double-queries the same store, almost certainly a misconfiguration")
	}
	if primary.Engine != "" && primary.Engine != c.engine() {
		warnings = append(warnings, fmt.Sprintf("secondary cache (%s, a %s cache) is paired with a %s primary: mixing cache engines across the two tiers is not a supported configuration (docs/SECONDARY-CACHE.md); both tiers still run",
			c.source(), c.engine(), primary.Engine))
	}
	return warnings, nil
}
