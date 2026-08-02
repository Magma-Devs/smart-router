package performance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Startup validation rules from docs/SECONDARY-CACHE-DESIGN.md §11 (T9).
func TestSecondaryCacheConfigValidate(t *testing.T) {
	valid := SecondaryCacheConfig{Address: "cache-shared:20100", Timeout: 75 * time.Millisecond, Mode: SecondaryCacheModeReadOnly}

	t.Run("disabled with no options is valid", func(t *testing.T) {
		warnings, err := SecondaryCacheConfig{Timeout: DefaultSecondaryCacheTimeout, Mode: SecondaryCacheModeReadOnly}.Validate("cache:20100", false, false)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	t.Run("dangling timeout without address fails", func(t *testing.T) {
		_, err := SecondaryCacheConfig{Timeout: time.Second, Mode: SecondaryCacheModeReadOnly}.Validate("cache:20100", true, false)
		require.ErrorContains(t, err, "dangling")
	})

	t.Run("dangling mode without address fails", func(t *testing.T) {
		_, err := SecondaryCacheConfig{Timeout: DefaultSecondaryCacheTimeout, Mode: SecondaryCacheModeReadOnly}.Validate("cache:20100", false, true)
		require.ErrorContains(t, err, "dangling")
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
