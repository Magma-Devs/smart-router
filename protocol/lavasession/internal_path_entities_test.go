package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

func pathEntity(name, path string) *ConsumerSessionsWithProvider {
	p := NewConsumerSessionWithProvider(name, []*Endpoint{{Connections: []*EndpointConnection{}, NetworkAddress: grpcListener, Enabled: true, InternalPath: path}}, 200, firstEpochHeight, 10)
	p.StaticProvider = true
	p.Parent = "ton"
	return p
}

func str(s string) *string { return &s }

// MAG-4219. A TON root url becomes three entities: ton/0 (the root), ton/0/v2 and ton/0/v3. Before
// the split one provider held the three urls and the endpoint walk preferred the url with the
// api's exact internal path; with one url per entity that preference has to be applied to the
// candidate list, or a v3 api lands on whichever entity the optimizer ranked first (found on the
// local TON rig, 2026-10-26).
func TestInternalPath_NarrowsCandidatesToTheEntityServingIt(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, map[uint64]*ConsumerSessionsWithProvider{
		0: pathEntity("ton/0", ""),
		1: pathEntity("ton/0/v2", "/v2"),
		2: pathEntity("ton/0/v3", "/v3"),
	}, nil))
	time.Sleep(5 * time.Millisecond)
	ctx := context.Background()
	pick := func(path *string) []string {
		got, err := csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "", path)
		require.NoError(t, err)
		return got
	}
	for range 20 {
		require.Equal(t, []string{"ton/0/v3"}, pick(str("/v3")))
		require.Equal(t, []string{"ton/0/v2"}, pick(str("/v2")))
		require.Equal(t, []string{"ton/0"}, pick(str("")), "the root collection goes to the root url")
	}

	// A path no entity serves narrows nothing: the fallback the endpoint walk always had.
	seen := map[string]bool{}
	for range 60 {
		seen[pick(str("/v9"))[0]] = true
	}
	require.Len(t, seen, 3, "no exact match: every entity stays a candidate")

	// A relay that resolved no collection narrows nothing either.
	seen = map[string]bool{}
	for range 60 {
		seen[pick(nil)[0]] = true
	}
	require.Len(t, seen, 3)

	csm.lock.RLock()
	defer csm.lock.RUnlock()
	require.Equal(t, []string{"ton/0/v2"}, csm.narrowToInternalPath([]string{"ton/0", "ton/0/v2", "ton/0/v3"}, str("/v2")))
	require.Equal(t, []string{"ton/0/v3"}, csm.narrowToInternalPath([]string{"ton/0/v3"}, str("/v3")))
	require.Equal(t, []string{"ton/0", "ton/0/v2"}, csm.narrowToInternalPath([]string{"ton/0", "ton/0/v2"}, str("/v3")), "nothing serves /v3 among these: unchanged")
}

// The pin header and the path compose: a configured name narrows to its urls, the path then picks
// the one serving the api.
func TestInternalPath_ComposesWithTheParentPin(t *testing.T) {
	csm := CreateConsumerSessionManager()
	other := pathEntity("tatum/0/v3", "/v3")
	other.Parent = "tatum"
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, map[uint64]*ConsumerSessionsWithProvider{
		0: pathEntity("ton/0", ""), 1: pathEntity("ton/0/v2", "/v2"), 2: pathEntity("ton/0/v3", "/v3"), 3: other,
	}, nil))
	time.Sleep(5 * time.Millisecond)
	for range 10 {
		got, err := csm.getValidProviderAddresses(context.Background(), 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "ton", str("/v3"))
		require.NoError(t, err)
		require.Equal(t, []string{"ton/0/v3"}, got)
	}
}
