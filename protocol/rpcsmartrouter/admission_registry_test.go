package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// refusing returns a real admission with the given node-url positions refused, from a
// pass over local upstreams: the refusals come from failed checks, as in production.
func refusing(t *testing.T, positions ...int) (chainlib.ProviderAdmission, int) {
	t.Helper()
	up := newFakeUpstreams(t)
	urls := make([]commonlib.NodeUrl, 3)
	for i := range urls {
		urls[i] = commonlib.NodeUrl{Url: up.httpURL("/r/" + string(rune('a'+i)) + "/" + upArchive)}
	}
	for _, position := range positions {
		urls[position].Url = up.httpURL("/r/" + string(rune('a'+position)) + "/" + upDead)
	}
	admission, err := validateProviderCollections(t.Context(), staticProvider("ETH1", urls...), liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	return admission, len(urls)
}

func TestAdmissionRegistry_AFirstRecordThatRefusesHasMoved(t *testing.T) {
	provider := staticProvider("ETH1")
	registry := newAdmissionRegistry()
	require.False(t, registry.record(provider, chainlib.ProviderAdmission{}), "nothing refused, nothing to rebuild")

	refused, _ := refusing(t, 1)
	other := staticProvider("ETH1")
	require.True(t, registry.record(other, refused), "a session built before this record holds the refused url")
}

// A url that fails an epoch pass for the first time keeps its place; the second pass in
// a row refuses it; a pass it answers clears the count.
func TestAdmissionRegistry_AUrlGetsTheProvidersGrace(t *testing.T) {
	provider := staticProvider("ETH1")
	registry := newAdmissionRegistry()
	registry.record(provider, chainlib.ProviderAdmission{})
	refused, _ := refusing(t, 2)

	require.False(t, registry.recordReverified(provider, refused), "first failed pass: graced")
	require.False(t, registry.admissionFor(provider).URLRefused(2))
	require.True(t, registry.recordReverified(provider, refused), "second failed pass in a row: refused")
	require.True(t, registry.admissionFor(provider).URLRefused(2))
	require.False(t, registry.recordReverified(provider, refused), "still refused, nothing moved")

	require.True(t, registry.recordReverified(provider, chainlib.ProviderAdmission{}), "it answered: admitted again")
	require.False(t, registry.recordReverified(provider, refused), "the count started over")
	require.False(t, registry.admissionFor(provider).URLRefused(2))
}

func TestAdmissionRegistry_ABootOrRetryRecordStandsAsIs(t *testing.T) {
	provider := staticProvider("ETH1")
	registry := newAdmissionRegistry()
	refused, _ := refusing(t, 0)
	registry.record(provider, refused)
	require.True(t, registry.admissionFor(provider).URLRefused(0), "no grace outside the epoch pass")
}
