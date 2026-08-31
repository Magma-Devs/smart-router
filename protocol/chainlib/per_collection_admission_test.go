package chainlib

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3326. Admission used to be all-or-nothing per provider: Validate returned
// on the first Fail-severity verification, so a provider serving the base
// collection perfectly and failing ONE add-on's verification was excluded from
// everything. Adding an archive url to a healthy provider could cost the
// provider.

func TestProviderAdmission_ApplyStripsOnlyTheRefusedAddon(t *testing.T) {
	urls := []common.NodeUrl{
		{Url: "https://a.example.com", Addons: []string{"archive", "debug"}},
		{Url: "https://b.example.com", Addons: []string{"archive"}},
		{Url: "https://c.example.com"},
	}

	var admission ProviderAdmission
	admission.fail(urls[0], "archive")

	got := admission.Apply(urls)
	require.Len(t, got, 3, "a refused add-on removes the add-on, never the url")
	require.Equal(t, []string{"debug"}, got[0].Addons, "only the refused add-on goes")
	require.Equal(t, []string{"archive"}, got[1].Addons, "a different url keeps the same add-on")
	require.Empty(t, got[2].Addons)

	// The parsed configuration must survive untouched: the epoch re-verification
	// path re-reads it every cycle, so stripping in place would make one transient
	// failure permanent until restart.
	require.Equal(t, []string{"archive", "debug"}, urls[0].Addons)
}

func TestProviderAdmission_KeyedByUrlAndInternalPath(t *testing.T) {
	urls := []common.NodeUrl{
		{Url: "https://a.example.com", InternalPath: "", Addons: []string{"archive"}},
		{Url: "https://a.example.com", InternalPath: "/v2", Addons: []string{"archive"}},
	}

	var admission ProviderAdmission
	admission.fail(urls[1], "archive")

	got := admission.Apply(urls)
	require.Equal(t, []string{"archive"}, got[0].Addons, "the root path keeps it")
	require.Empty(t, got[1].Addons, "only the internal path that failed loses it")
}

func TestProviderAdmission_EmptyIsIdentity(t *testing.T) {
	urls := []common.NodeUrl{{Url: "https://a.example.com", Addons: []string{"archive"}}}
	var admission ProviderAdmission
	require.False(t, admission.Any())
	got := admission.Apply(urls)
	require.Equal(t, urls, got, "nothing refused means nothing changes, and no copy is needed")
}

// TestProviderAdmission_AnyReportsRefusals covers the signal validateProviderTier
// uses to decide whether a provider needs its urls rewritten at all.
func TestProviderAdmission_AnyReportsRefusals(t *testing.T) {
	var admission ProviderAdmission
	require.False(t, admission.Any())
	admission.fail(common.NodeUrl{Url: "https://a.example.com"}, "archive")
	require.True(t, admission.Any())
}
