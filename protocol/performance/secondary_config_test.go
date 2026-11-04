package performance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Startup validation rules (docs/SECONDARY-CACHE.md).
func TestSecondaryCacheConfigValidate(t *testing.T) {
	valid := SecondaryCacheConfig{Address: "cache-shared:20100", Timeout: 75 * time.Millisecond, Mode: SecondaryCacheModeReadOnly}

	t.Run("disabled with no options is valid", func(t *testing.T) {
		warnings, err := SecondaryCacheConfig{Timeout: DefaultSecondaryCacheTimeout, Mode: SecondaryCacheModeReadOnly}.Validate(SecondaryCachePrimary{Address: "cache:20100", Engine: CacheEngineGRPC}, false, false)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	// Dangling options warn instead of failing startup: the shape is usually a typo,
	// but it also occurs legitimately when one templated YAML is shared across a fleet
	// where only some routers run a secondary. Failing there would turn an unused key
	// into an outage on every router that does not.
	t.Run("dangling timeout without address warns and starts", func(t *testing.T) {
		warnings, err := SecondaryCacheConfig{Timeout: time.Second, Mode: SecondaryCacheModeReadOnly}.Validate(SecondaryCachePrimary{Address: "cache:20100", Engine: CacheEngineGRPC}, true, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "dangling")
	})

	t.Run("dangling mode without address warns and starts", func(t *testing.T) {
		warnings, err := SecondaryCacheConfig{Timeout: DefaultSecondaryCacheTimeout, Mode: SecondaryCacheModeReadOnly}.Validate(SecondaryCachePrimary{Address: "cache:20100", Engine: CacheEngineGRPC}, false, true)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "dangling")
	})

	t.Run("zero timeout fails", func(t *testing.T) {
		cfg := valid
		cfg.Timeout = 0
		_, err := cfg.Validate(SecondaryCachePrimary{Address: "cache:20100", Engine: CacheEngineGRPC}, true, false)
		require.ErrorContains(t, err, SecondaryCacheTimeoutFlagName)
	})

	t.Run("negative timeout fails", func(t *testing.T) {
		cfg := valid
		cfg.Timeout = -time.Millisecond
		_, err := cfg.Validate(SecondaryCachePrimary{Address: "cache:20100", Engine: CacheEngineGRPC}, true, false)
		require.ErrorContains(t, err, SecondaryCacheTimeoutFlagName)
	})

	t.Run("read-write mode is rejected in v1", func(t *testing.T) {
		cfg := valid
		cfg.Mode = "read-write"
		_, err := cfg.Validate(SecondaryCachePrimary{Address: "cache:20100", Engine: CacheEngineGRPC}, false, true)
		require.ErrorContains(t, err, "reserved")
	})

	t.Run("secondary without primary is allowed with warning", func(t *testing.T) {
		warnings, err := valid.Validate(SecondaryCachePrimary{}, false, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "without a primary")
	})

	t.Run("same address as primary warns", func(t *testing.T) {
		warnings, err := valid.Validate(SecondaryCachePrimary{Address: valid.Address, Engine: CacheEngineGRPC}, false, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "equals the primary")
	})

	t.Run("happy path with distinct primary", func(t *testing.T) {
		warnings, err := valid.Validate(SecondaryCachePrimary{Address: "cache-internal:20100", Engine: CacheEngineGRPC}, true, true)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	// MAG-4777: the secondary is always a gRPC cache, so a RESP primary mixes engines.
	// Warned, never refused — both tiers still run.
	t.Run("RESP primary with a secondary warns and does not fail", func(t *testing.T) {
		warnings, err := valid.Validate(SecondaryCachePrimary{Address: "valkey:6379", Engine: CacheEngineRESP}, false, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "mixing cache engines")
		require.Contains(t, warnings[0], SecondaryCacheFlagName)
	})

	// MAG-4778: a RESP secondary beside a RESP primary is the supported pairing.
	respSecondary := SecondaryCacheConfig{Address: "zone-a-valkey:6379", Timeout: DefaultSecondaryCacheTimeout, Mode: SecondaryCacheModeReadOnly, Engine: CacheEngineRESP, KeyPrefix: "zone-a"}

	t.Run("RESP secondary beside a RESP primary does not warn", func(t *testing.T) {
		warnings, err := respSecondary.Validate(SecondaryCachePrimary{Address: "zone-b-valkey:6379", Engine: CacheEngineRESP, KeyPrefix: "zone-b"}, false, false)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	t.Run("RESP secondary beside a gRPC primary warns about the mixed engines", func(t *testing.T) {
		warnings, err := respSecondary.Validate(SecondaryCachePrimary{Address: "cache:20100", Engine: CacheEngineGRPC}, false, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "mixing cache engines")
		require.Contains(t, warnings[0], SecondaryRespCacheViperKey)
	})

	// Two zones may share one backend under different prefixes; only the same
	// keyspace on the same address is the primary's own store.
	t.Run("RESP secondary on the primary's backend but another keyspace does not warn", func(t *testing.T) {
		warnings, err := respSecondary.Validate(SecondaryCachePrimary{Address: respSecondary.Address, Engine: CacheEngineRESP, KeyPrefix: "zone-b"}, false, false)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	t.Run("RESP secondary that is the primary's own keyspace warns", func(t *testing.T) {
		warnings, err := respSecondary.Validate(SecondaryCachePrimary{Address: respSecondary.Address, Engine: CacheEngineRESP, KeyPrefix: "zone-a"}, false, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "equals the primary")
	})

	t.Run("RESP primary without a secondary does not warn", func(t *testing.T) {
		warnings, err := SecondaryCacheConfig{}.Validate(SecondaryCachePrimary{Address: "valkey:6379", Engine: CacheEngineRESP}, false, false)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})
}
