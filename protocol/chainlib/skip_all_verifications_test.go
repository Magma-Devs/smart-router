package chainlib

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// withSkipAll flips the process-wide flag for one test and restores it, so a failure
// cannot leak the "verification is off" state into every later test in the package.
func withSkipAll(t *testing.T, v bool) {
	t.Helper()
	prev := SkipAllVerifications
	SkipAllVerifications = v
	t.Cleanup(func() { SkipAllVerifications = prev })
}

// skipVerification is the single gate every verification-driven probe consults. The flag
// and the per-node-url config are independent sources of suppression; neither may mask a
// bug in the other.
func TestSkipVerification_FlagAndConfig(t *testing.T) {
	plain := common.NodeUrl{Url: "https://example.invalid"}
	named := common.NodeUrl{Url: "https://example.invalid", SkipVerifications: []string{"pruning"}}
	wild := common.NodeUrl{Url: "https://example.invalid", SkipVerifications: []string{common.SkipVerificationsWildcard}}

	t.Run("flag off: nothing is skipped without config", func(t *testing.T) {
		withSkipAll(t, false)
		require.False(t, skipVerification(plain, "pruning"))
		require.False(t, skipVerification(plain, "chain-id"))
	})

	t.Run("flag off: config still governs", func(t *testing.T) {
		withSkipAll(t, false)
		require.True(t, skipVerification(named, "pruning"))
		require.False(t, skipVerification(named, "chain-id"), "a named skip must not spill onto other verifications")
		require.True(t, skipVerification(wild, "chain-id"), "the config wildcard still works with the flag off")
	})

	t.Run("flag on: every verification is skipped, config irrelevant", func(t *testing.T) {
		withSkipAll(t, true)
		require.True(t, skipVerification(plain, "pruning"))
		require.True(t, skipVerification(plain, "chain-id"))
		require.True(t, skipVerification(plain, "some-verification-nobody-enumerated"))
		require.True(t, skipVerification(named, "chain-id"))
	})

	t.Run("flag defaults to off", func(t *testing.T) {
		require.False(t, SkipAllVerifications, "the process-wide switch must be opt-in")
	})
}

// The flag has to suppress the latest-block probe too, not just the verifications. That probe
// is a real relay whose failure aborts Validate and demotes the provider, so a flag that left
// it running would not actually stop the router touching the upstream.
func TestSkipAllVerifications_SuppressesLatestBlockFetch(t *testing.T) {
	url := common.NodeUrl{Url: "https://example.invalid"}
	headDependent := []VerificationContainer{{Name: "pruning", LatestDistance: 100}}

	withSkipAll(t, false)
	require.True(t, needsLatestBlock(url, headDependent), "baseline: a live head-dependent verification needs the fetch")

	withSkipAll(t, true)
	require.False(t, needsLatestBlock(url, headDependent), "the flag must drop the latest-block probe as well")
}
