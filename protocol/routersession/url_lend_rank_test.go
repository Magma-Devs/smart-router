package routersession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestURLLendRank pins the ranking the subscription managers read to place a new subscription on a
// url in rotation (MAG-4161). A url takes the rank relay selection lends it by (lendRankLocked): 0 in
// rotation, 1 head-stalled, 2 switched off, in the pairing and the backup list alike, and the best
// rank of the endpoints at it. A websocket url, which nothing ever takes out, takes its provider's
// rank: the best of the provider's urls that can take a relay. A url with no endpoint ranks 0.
func TestURLLendRank(t *testing.T) {
	ctx := context.Background()
	connection := func(url string) DirectRPCConnection {
		conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 1, "")
		require.NoError(t, err)
		return conn
	}
	endpoint := func(url string, enabled bool) *Endpoint {
		return &Endpoint{NetworkAddress: url, Enabled: enabled, DirectConnections: []DirectRPCConnection{connection(url)}}
	}
	stalled := func(url string) *Endpoint {
		ep := endpoint(url, true)
		ep.AddStateReason(EndpointDisableHeadStalled, time.Now(), 100)
		return ep
	}
	headStalled := stalled("https://stalled.example.com")

	csm := CreateConsumerSessionManager()
	csm.lock.Lock()
	csm.pairing = map[string]*ConsumerSessionsWithProvider{
		"primary-1": {PublicAddress: "primary-1", Endpoints: []*Endpoint{
			endpoint("https://usable.example.com", true),
			endpoint("https://switched-off.example.com", false),
			headStalled,
		}},
		// The chart writes an archive node-url twice: one url, two endpoints.
		"primary-2": {PublicAddress: "primary-2", Endpoints: []*Endpoint{
			endpoint("https://archive-half-off.example.com", false),
			endpoint("https://archive-half-off.example.com", true),
			endpoint("https://archive-off-and-stalled.example.com", false),
			stalled("https://archive-off-and-stalled.example.com"),
		}},
		// https + wss providers: the wss url takes the rank of the provider's relay urls.
		"ws-https-up": {PublicAddress: "ws-https-up", Endpoints: []*Endpoint{
			endpoint("https://ws-https-up.example.com", true),
			endpoint("wss://ws-https-up.example.com", true),
		}},
		"ws-https-stalled": {PublicAddress: "ws-https-stalled", Endpoints: []*Endpoint{
			stalled("https://ws-https-stalled.example.com"),
			endpoint("wss://ws-https-stalled.example.com", true),
		}},
		"ws-https-off": {PublicAddress: "ws-https-off", Endpoints: []*Endpoint{
			endpoint("https://ws-https-off.example.com", false),
			endpoint("wss://ws-https-off.example.com", true),
		}},
		"ws-one-https-up": {PublicAddress: "ws-one-https-up", Endpoints: []*Endpoint{
			endpoint("https://ws-one-https-up-a.example.com", false),
			endpoint("https://ws-one-https-up-b.example.com", true),
			endpoint("wss://ws-one-https-up.example.com", true),
		}},
		// A websocket-only provider is judged by its own url.
		"ws-only": {PublicAddress: "ws-only", Endpoints: []*Endpoint{
			endpoint("wss://ws-only.example.com", true),
		}},
	}
	csm.backupProviders = map[string]*ConsumerSessionsWithProvider{
		"backup-1": {PublicAddress: "backup-1", Endpoints: []*Endpoint{
			endpoint("https://backup-usable.example.com", true),
			endpoint("https://backup-switched-off.example.com", false),
			endpoint("wss://backup-ws.example.com", true),
		}},
	}
	csm.lock.Unlock()

	for url, want := range map[string]int{
		"https://usable.example.com":                  0,
		"https://stalled.example.com":                 1,
		"https://switched-off.example.com":            2,
		"https://archive-half-off.example.com":        0,
		"https://archive-off-and-stalled.example.com": 1,
		"wss://ws-https-up.example.com":               0,
		"wss://ws-https-stalled.example.com":          1,
		"wss://ws-https-off.example.com":              2,
		"wss://ws-one-https-up.example.com":           0,
		"wss://ws-only.example.com":                   0,
		"https://backup-usable.example.com":           0,
		"https://backup-switched-off.example.com":     2,
		"wss://backup-ws.example.com":                 0,
		"https://not-paired.example.com":              0,
	} {
		assert.Equal(t, want, csm.URLLendRank(url), url)
	}

	// Read at call time: the url is back in rotation once the state reason comes off.
	headStalled.RemoveStateReason(EndpointDisableHeadStalled)
	assert.Equal(t, 0, csm.URLLendRank("https://stalled.example.com"), "a url whose head-stall cleared is back in rotation")
}
