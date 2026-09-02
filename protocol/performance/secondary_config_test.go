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
		warnings, err := SecondaryCacheConfig{Timeout: DefaultSecondaryCacheTimeout, Mode: SecondaryCacheModeReadOnly}.Validate("cache:20100", false, false)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	// Dangling options warn instead of failing startup: the shape is usually a typo,
	// but it also occurs legitimately when one templated YAML is shared across a fleet
	// where only some routers run a secondary. Failing there would turn an unused key
	// into an outage on every router that does not.
	t.Run("dangling timeout without address warns and starts", func(t *testing.T) {
		warnings, err := SecondaryCacheConfig{Timeout: time.Second, Mode: SecondaryCacheModeReadOnly}.Validate("cache:20100", true, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "dangling")
	})

	t.Run("dangling mode without address warns and starts", func(t *testing.T) {
		warnings, err := SecondaryCacheConfig{Timeout: DefaultSecondaryCacheTimeout, Mode: SecondaryCacheModeReadOnly}.Validate("cache:20100", false, true)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "dangling")
	})

	t.Run("zero timeout fails", func(t *testing.T) {
		cfg := valid
		cfg.Timeout = 0
		_, err := cfg.Validate("cache:20100", true, false)
		require.ErrorContains(t, err, SecondaryCacheTimeoutFlagName)
	})

	t.Run("negative timeout fails", func(t *testing.T) {
		cfg := valid
		cfg.Timeout = -time.Millisecond
		_, err := cfg.Validate("cache:20100", true, false)
		require.ErrorContains(t, err, SecondaryCacheTimeoutFlagName)
	})

	t.Run("read-write mode is rejected in v1", func(t *testing.T) {
		cfg := valid
		cfg.Mode = "read-write"
		_, err := cfg.Validate("cache:20100", false, true)
		require.ErrorContains(t, err, "reserved")
	})

	t.Run("secondary without primary is allowed with warning", func(t *testing.T) {
		warnings, err := valid.Validate("", false, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "without a primary")
	})

	t.Run("same address as primary warns", func(t *testing.T) {
		warnings, err := valid.Validate(valid.Address, false, false)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "equals the primary")
	})

	t.Run("happy path with distinct primary", func(t *testing.T) {
		warnings, err := valid.Validate("cache-internal:20100", true, true)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})
}
