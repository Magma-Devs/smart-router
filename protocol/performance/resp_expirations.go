package performance

import (
	"fmt"
	"math"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/spf13/viper"
)

// Expiration keys of the `resp-cache:` block. They are the cache server's
// expiration FLAG names verbatim (ecosystem/cache/command.go), so an operator
// moving off the sidecar transfers the same numbers under the same names
// rather than learning a second vocabulary for the same table.
const (
	RespCacheExpirationKey                       = "expiration"
	RespCacheExpirationMultiplierKey             = "expiration-multiplier"
	RespCacheExpirationNonFinalizedKey           = "expiration-non-finalized"
	RespCacheExpirationNonFinalizedMultiplierKey = "expiration-non-finalized-multiplier"
	RespCacheExpirationNodeErrorsKey             = "expiration-finalized-node-errors"
	RespCacheExpirationBlocksHashesToHeightsKey  = "expiration-blocks-hashes-to-heights"
)

// RespCacheExpirations is the TTL surface of the `resp-cache:` block: the
// route by which a router-embedded backend's core.Policy can be something
// other than core.DefaultPolicy().
//
// It exists because the two cache paths own their TTL table differently. The
// sidecar builds a Policy from its own flags, which the chart sets
// (expiration_multiplier: 1.5, expiration_non_finalized_multiplier: 1.25).
// A router on the RESP backend builds the Policy itself, in-process, where no
// cache-server flag reaches — so before this block the chart's two multipliers
// had no route in at all and every RESP deployment silently ran the unscaled
// defaults. Same engine, same rule, unreachable knob.
//
// Every field is optional and a field left out keeps the cache server's flag
// default, so a block that sets only connection details behaves exactly as it
// did before this type existed.
type RespCacheExpirations struct {
	// Finalized is the base TTL for an entry about a block that can no longer
	// change; FinalizedMultiplier scales it, mirroring the sidecar's
	// ExpirationFinalized = expiration * multiplier (ecosystem/cache/server.go).
	Finalized           time.Duration `mapstructure:"expiration"`
	FinalizedMultiplier float64       `mapstructure:"expiration-multiplier"`

	// NonFinalized is a FLOOR, not a duration: the effective TTL is
	// max(averageBlockTime/8, NonFinalized) per chain (core.Policy.ForChain).
	// It therefore only bites on chains whose block time is under 8x it —
	// which is most of specs/, so it is not the inert knob it looks like.
	NonFinalized           time.Duration `mapstructure:"expiration-non-finalized"`
	NonFinalizedMultiplier float64       `mapstructure:"expiration-non-finalized-multiplier"`

	// NodeErrors bounds how long a transient upstream error is served; the
	// engine additionally caps it at one block time.
	NodeErrors time.Duration `mapstructure:"expiration-finalized-node-errors"`
	// BlocksHashesToHeights is the block-hash→height mapping TTL.
	BlocksHashesToHeights time.Duration `mapstructure:"expiration-blocks-hashes-to-heights"`
}

// respCacheExpirationValue is one configured expiration alongside the YAML key
// it came from, so validation and logging can name the key an operator
// actually wrote rather than a Go field name they will not find in their
// config. Durations and multipliers share one type because every check here is
// on the sign, which they answer alike.
type respCacheExpirationValue struct {
	key   string
	value float64
}

// valuesByKey enumerates the block's expiration keys in config order.
func (e RespCacheExpirations) valuesByKey() []respCacheExpirationValue {
	return []respCacheExpirationValue{
		{RespCacheExpirationKey, float64(e.Finalized)},
		{RespCacheExpirationMultiplierKey, e.FinalizedMultiplier},
		{RespCacheExpirationNonFinalizedKey, float64(e.NonFinalized)},
		{RespCacheExpirationNonFinalizedMultiplierKey, e.NonFinalizedMultiplier},
		{RespCacheExpirationNodeErrorsKey, float64(e.NodeErrors)},
		{RespCacheExpirationBlocksHashesToHeightsKey, float64(e.BlocksHashesToHeights)},
	}
}

// LoadRespCacheExpirations reads the expiration keys of the `resp-cache:`
// block. An absent key is not an error — it keeps the cache server's default.
//
// A key that is PRESENT and non-positive is rejected rather than clamped or
// ignored, which is the one place this deliberately does not mirror the cache
// server. The sidecar accepts --expiration-multiplier 0 and produces a zero
// TTL; ristretto reads that as "never expires" and the entry outlives the pod
// that wrote it, which is contained because the store dies with the pod. On a
// RESP backend it is not contained: go-redis maps a zero expiration to a key
// with no TTL, written into a store the customer owns and may share, where it
// survives every restart and is reclaimed only by eviction. Turning a typo
// into an unbounded keyspace is worth a startup error.
func LoadRespCacheExpirations(v *viper.Viper) (RespCacheExpirations, error) {
	var expirations RespCacheExpirations
	if !v.IsSet(RespCacheViperKey) {
		return expirations, nil
	}
	if err := v.UnmarshalKey(RespCacheViperKey, &expirations); err != nil {
		return RespCacheExpirations{}, fmt.Errorf("invalid %s expirations: %w", RespCacheViperKey, err)
	}
	for _, field := range expirations.valuesByKey() {
		// IsSet, not value != 0: an omitted key and an explicit zero both
		// unmarshal to zero, and only one of them is a mistake.
		if !v.IsSet(RespCacheViperKey+"."+field.key) || field.value > 0 {
			continue
		}
		return RespCacheExpirations{}, fmt.Errorf(
			"%s.%s must be greater than zero (got %v) — a zero or negative expiration writes keys with no TTL, which never expire in a RESP backend; remove the key to use the default",
			RespCacheViperKey, field.key, v.Get(RespCacheViperKey+"."+field.key))
	}
	return expirations, nil
}

// Policy resolves the configured expirations into the TTL table the engine
// writes with: each base falls back to the cache server's flag default, then
// its multiplier scales it exactly as the sidecar does. The multipliers are
// applied AFTER the base is resolved, so setting only a multiplier scales the
// default — which is how the chart's 1.5 and 1.25 are meant to be carried over.
//
// This changes nothing about how a TTL is chosen. core.Policy.ForRelayEntry
// and ForChain are untouched; this only decides what numbers they are handed.
func (e RespCacheExpirations) Policy() core.Policy {
	policy := core.DefaultPolicy()
	if e.Finalized > 0 {
		policy.Finalized = e.Finalized
	}
	if e.NonFinalized > 0 {
		policy.NonFinalized = e.NonFinalized
	}
	if e.NodeErrors > 0 {
		policy.NodeErrors = e.NodeErrors
	}
	if e.BlocksHashesToHeights > 0 {
		policy.BlocksHashesToHeights = e.BlocksHashesToHeights
	}
	if e.FinalizedMultiplier > 0 {
		policy.Finalized = scaleExpiration(policy.Finalized, e.FinalizedMultiplier)
	}
	if e.NonFinalizedMultiplier > 0 {
		policy.NonFinalized = scaleExpiration(policy.NonFinalized, e.NonFinalizedMultiplier)
	}
	return policy
}

// configured reports whether any expiration key was supplied, so the startup
// log can distinguish a deployment that carried its TTLs over from one running
// the defaults — the distinction this ticket is about, made visible at the
// moment it is decided.
func (e RespCacheExpirations) configured() bool {
	for _, field := range e.valuesByKey() {
		if field.value > 0 {
			return true
		}
	}
	return false
}

// scaleExpiration multiplies a TTL, saturating rather than wrapping. The
// product of a large duration and a large multiplier overflows int64 and lands
// NEGATIVE, which go-redis rejects and ristretto treats as already expired —
// both worse than the absurd-but-honest ~292 years MaxInt64 represents. The
// validation above bounds the multiplier's sign, not its size.
func scaleExpiration(base time.Duration, multiplier float64) time.Duration {
	scaled := float64(base) * multiplier
	if scaled >= math.MaxInt64 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(scaled)
}
