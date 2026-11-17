package endpointstate

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The cases below use the bundled ETH1 spec. Its `bundler` add-on collection declares no
// GET_BLOCKNUM, so a url that serves ONLY `bundler` (standalone-addons) has no way to read the
// head: the shape of a transaction-submit service behind an add-on (MAG-5349).

// countingConnection counts every request the monitor sends to a url.
type countingConnection struct {
	mockDirectRPCConnection
	sent atomic.Int32
}

func (c *countingConnection) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*routersession.DirectRPCResponse, error) {
	c.sent.Add(1)
	return c.mockDirectRPCConnection.SendRequest(ctx, data, headers)
}

// headLookupCountingParser counts the collection-scoped GET_BLOCKNUM lookups, and can answer
// them itself to stand in for an add-on collection that does declare the head.
type headLookupCountingParser struct {
	chainlib.ChainParser
	lookups     atomic.Int32
	addonHasTag bool
}

func (p *headLookupCountingParser) GetParsingByTagForCollection(tag spectypes.FUNCTION_TAG, addons []string, internalPath string, allowBaseFallback bool) (*spectypes.ParseDirective, *spectypes.ApiCollection, bool) {
	if tag == spectypes.FUNCTION_TAG_GET_BLOCKNUM {
		p.lookups.Add(1)
		if p.addonHasTag {
			return p.ChainParser.GetParsingByTag(tag)
		}
	}
	return p.ChainParser.GetParsingByTagForCollection(tag, addons, internalPath, allowBaseFallback)
}

func newETHMonitor(t *testing.T, ctx context.Context, parser chainlib.ChainParser, chainID string) *EndpointMonitor {
	t.Helper()
	ensureRandSeeded()
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          chainID,
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

func bundlerEndpoint(url string, standalone bool) *routersession.Endpoint {
	return &routersession.Endpoint{
		NetworkAddress:   url,
		Enabled:          true,
		Addons:           map[string]struct{}{"bundler": {}},
		StandaloneAddons: standalone,
	}
}

// A url that serves only an add-on with no head method gets no tracker, sends nothing, and the
// decision is made once however many relays ask for its tracker.
func TestUntrackedByDesign_StandaloneAddonWithoutHead_GetsNoTracker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parser := &headLookupCountingParser{ChainParser: newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)}
	m := newETHMonitor(t, ctx, parser, "ETH1")

	url := "http://bundler-only:4337"
	conn := &countingConnection{mockDirectRPCConnection: mockDirectRPCConnection{url: url}}
	endpoint := bundlerEndpoint(url, true)

	for i := 0; i < 50; i++ {
		tracker, err := m.GetOrCreateTracker("bundler-provider", endpoint, conn)
		require.NoError(t, err)
		require.Nil(t, tracker, "no tracker for a url that has no head by design")
	}

	require.Equal(t, int32(1), parser.lookups.Load(), "the decision (and its log line) is made once per url, not once per call")
	require.True(t, m.IsUntrackedByDesign(url))
	_, exists := m.GetTracker(url)
	require.False(t, exists)
	require.Equal(t, 0, m.GetEndpointCount(), "it is not counted as an active tracker")

	state, lastErr, tracked := m.GetTrackerState(url)
	require.Equal(t, EndpointChainTrackerNotTrackedByDesign, state)
	require.Empty(t, lastErr, "by design is not an error")
	require.False(t, tracked)

	_, hasGeneration := m.ObservationGeneration(url)
	require.False(t, hasGeneration, "nothing may record an observation for it")

	// A tracker would have sent its first poll well within this.
	time.Sleep(300 * time.Millisecond)
	require.Zero(t, conn.sent.Load(), "no request reaches a url that is not tracked")
}

// The url is forgotten when it leaves the config, through the same calls that remove a tracker.
func TestUntrackedByDesign_IsListedAndForgotten(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newETHMonitor(t, ctx, newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC), "ETH1")

	url := "http://bundler-only:4337"
	conn := &mockDirectRPCConnection{url: url}
	_, err := m.GetOrCreateTracker("", bundlerEndpoint(url, true), conn)
	require.NoError(t, err)

	require.Contains(t, m.GetAllEndpoints(), url, "the stale-url cleanup must see it to drop it")

	m.RemoveTracker(url)
	require.False(t, m.IsUntrackedByDesign(url))
	require.NotContains(t, m.GetAllEndpoints(), url)
	state, _, _ := m.GetTrackerState(url)
	require.Equal(t, EndpointChainTrackerMissing, state)

	// Configured again, it is decided again.
	_, err = m.GetOrCreateTracker("", bundlerEndpoint(url, true), conn)
	require.NoError(t, err)
	require.True(t, m.IsUntrackedByDesign(url))
}

// The rule is narrow: every other url keeps its tracker, including the ones whose tracker
// cannot start. Those are faults, and must stay loud.
func TestUntrackedByDesign_OnlyTheNarrowCase(t *testing.T) {
	realParser := func(t *testing.T) chainlib.ChainParser {
		return newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)
	}
	cases := []struct {
		name     string
		chainID  string
		parser   func(t *testing.T) chainlib.ChainParser
		endpoint *routersession.Endpoint
	}{
		{
			name:     "no add-ons: serves the base collection",
			chainID:  "ETH1",
			parser:   realParser,
			endpoint: &routersession.Endpoint{NetworkAddress: "http://plain:8545", Enabled: true},
		},
		{
			name:     "add-on without standalone-addons: the base head is its head",
			chainID:  "ETH1",
			parser:   realParser,
			endpoint: bundlerEndpoint("http://full-plus-bundler:8545", false),
		},
		{
			name:    "standalone add-on whose own collection declares the head",
			chainID: "ETH1",
			parser: func(t *testing.T) chainlib.ChainParser {
				return &headLookupCountingParser{ChainParser: realParser(t), addonHasTag: true}
			},
			endpoint: bundlerEndpoint("http://addon-with-head:8545", true),
		},
		{
			// A custom-wrapper chain reads the head with its own fixed request, not the
			// directive, so the directive's absence says nothing about the url.
			name:     "standalone add-on on a chain that does not poll by directive",
			chainID:  "SOLANA",
			parser:   realParser,
			endpoint: bundlerEndpoint("http://svm-addon:8899", true),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			m := newETHMonitor(t, ctx, c.parser(t), c.chainID)

			conn := &mockDirectRPCConnection{url: c.endpoint.NetworkAddress}
			tracker, err := m.GetOrCreateTracker("", c.endpoint, conn)
			require.NoError(t, err)
			require.NotNil(t, tracker, "this url must keep its tracker")
			require.False(t, m.IsUntrackedByDesign(c.endpoint.NetworkAddress))
			state, _, tracked := m.GetTrackerState(c.endpoint.NetworkAddress)
			require.True(t, tracked)
			require.NotEqual(t, EndpointChainTrackerNotTrackedByDesign, state)
		})
	}
}

func TestIsUntrackedByDesign_NilMonitor(t *testing.T) {
	var m *EndpointMonitor
	require.False(t, m.IsUntrackedByDesign("http://anything"))
}

// Two providers can list one url: one as a base url, the other as a standalone add-on url with
// no head. The decision is kept per url, so it must follow the endpoint that can read the head,
// whichever of the two is seen first. Otherwise the base provider loses its block height to the
// other provider's opt-out.
func TestUntrackedByDesign_SharedURL_FollowsTheEndpointThatHasAHead(t *testing.T) {
	const url = "http://shared-node:8545"
	baseEndpoint := &routersession.Endpoint{NetworkAddress: url, Enabled: true}

	for _, standaloneFirst := range []bool{true, false} {
		name := "base url seen first"
		if standaloneFirst {
			name = "standalone add-on url seen first"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			parser := &headLookupCountingParser{ChainParser: newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)}
			m := newETHMonitor(t, ctx, parser, "ETH1")
			conn := &mockDirectRPCConnection{url: url}
			standalone := bundlerEndpoint(url, true)

			if standaloneFirst {
				tracker, err := m.GetOrCreateTracker("addon-provider", standalone, conn)
				require.NoError(t, err)
				require.Nil(t, tracker)
				require.True(t, m.IsUntrackedByDesign(url), "precondition: alone, it is not tracked")
				require.True(t, m.IsSettledUntracked(standalone))
				require.False(t, m.IsSettledUntracked(baseEndpoint), "the base endpoint is not settled: it must still get the url its tracker")
			}

			tracker, err := m.GetOrCreateTracker("base-provider", baseEndpoint, conn)
			require.NoError(t, err)
			require.NotNil(t, tracker, "the url is a base url of another provider: it is tracked")

			// The standalone endpoint, seen after, shares that tracker.
			again, err := m.GetOrCreateTracker("addon-provider", standalone, conn)
			require.NoError(t, err)
			require.Same(t, tracker, again)

			require.False(t, m.IsUntrackedByDesign(url))
			require.False(t, m.IsSettledUntracked(standalone))
			require.Equal(t, []string{url}, m.GetAllEndpoints(), "the url is listed once: it is in one of the two maps")
			state, _, tracked := m.GetTrackerState(url)
			require.True(t, tracked)
			require.NotEqual(t, EndpointChainTrackerNotTrackedByDesign, state)
		})
	}
}

// A second standalone endpoint with no head on a url already held as not tracked changes
// nothing, and each endpoint shape costs one spec lookup however often it is asked.
func TestUntrackedByDesign_SharedURL_TwoHeadlessEndpointsStayUntracked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parser := &headLookupCountingParser{ChainParser: newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)}
	m := newETHMonitor(t, ctx, parser, "ETH1")

	const url = "http://addons-node:8545"
	conn := &mockDirectRPCConnection{url: url}
	bundler := bundlerEndpoint(url, true)
	debug := &routersession.Endpoint{NetworkAddress: url, Enabled: true, Addons: map[string]struct{}{"debug": {}}, StandaloneAddons: true}

	for i := 0; i < 20; i++ {
		for _, endpoint := range []*routersession.Endpoint{bundler, debug} {
			tracker, err := m.GetOrCreateTracker("p", endpoint, conn)
			require.NoError(t, err)
			require.Nil(t, tracker)
		}
	}
	require.True(t, m.IsUntrackedByDesign(url))
	require.True(t, m.IsSettledUntracked(bundler))
	require.True(t, m.IsSettledUntracked(debug))
	require.Equal(t, int32(2), parser.lookups.Load(), "one lookup per endpoint shape, not per call")
}
