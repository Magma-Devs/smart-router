package rpcsmartrouter

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// routerIDShape is the rs_ id format: a 6-character hex hash of the client, then a counter
// of at least 5 digits.
var routerIDShape = regexp.MustCompile(`^rs_[0-9a-f]{6}_[0-9]{5,}$`)

// routerIDCounter returns the counter part of an rs_ id.
func routerIDCounter(t *testing.T, id string) uint64 {
	t.Helper()
	require.Regexp(t, routerIDShape, id)
	counter, err := strconv.ParseUint(id[strings.LastIndex(id, "_")+1:], 10, 64)
	require.NoError(t, err)
	return counter
}

func TestSubscriptionIDMapper_GenerateRouterID(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	clientKey := "dapp123:192.168.1.1:ws-uid-456"
	id1 := mapper.GenerateRouterID(clientKey)
	id2 := mapper.GenerateRouterID(clientKey)
	id3 := mapper.GenerateRouterID(clientKey)

	// Format: rs_{hash}_{counter}, one hash per client
	for _, id := range []string{id1, id2, id3} {
		assert.Regexp(t, routerIDShape, id)
		assert.Equal(t, strings.Split(id1, "_")[1], strings.Split(id, "_")[1], "one client keeps one hash")
	}

	// Unique, and the counter only grows
	c1, c2, c3 := routerIDCounter(t, id1), routerIDCounter(t, id2), routerIDCounter(t, id3)
	assert.Less(t, c1, c2, "the counter must grow")
	assert.Less(t, c2, c3, "the counter must grow")
}

func TestSubscriptionIDMapper_GenerateRouterID_DifferentClients(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	client1 := "client1"
	client2 := "client2"

	id1a := mapper.GenerateRouterID(client1)
	id1b := mapper.GenerateRouterID(client1)
	id2a := mapper.GenerateRouterID(client2)
	id2b := mapper.GenerateRouterID(client2)

	// One counter numbers every id the mapper issues, so no two clients can ever be
	// handed the same id and nothing per client has to be released (MAG-3722).
	counters := []uint64{
		routerIDCounter(t, id1a), routerIDCounter(t, id1b),
		routerIDCounter(t, id2a), routerIDCounter(t, id2b),
	}
	for i := 1; i < len(counters); i++ {
		assert.Less(t, counters[i-1], counters[i], "one counter grows across clients")
	}

	// Different hash prefixes for different clients
	parts1 := strings.Split(id1a, "_")
	parts2 := strings.Split(id2a, "_")
	assert.NotEqual(t, parts1[1], parts2[1], "different clients should have different hash")
}

// TestSubscriptionIDMapper_GenerateRouterID_StepIsNotACount is MAG-5438's regression. The
// counter is shared by every client, so with a +1 step a client that subscribes, waits while
// one other client subscribes, then subscribes again always sees its counter move by 2: the
// step tells it how many ids went to others in between. With a random step the move varies.
func TestSubscriptionIDMapper_GenerateRouterID_StepIsNotACount(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	moves := make(map[uint64]struct{})
	for i := 0; i < 20; i++ {
		first := routerIDCounter(t, mapper.GenerateRouterID("watcher"))
		mapper.GenerateRouterID("someone-else")
		second := routerIDCounter(t, mapper.GenerateRouterID("watcher"))
		require.Greater(t, second, first, "the counter must grow")
		moves[second-first] = struct{}{}
	}

	assert.Greater(t, len(moves), 1,
		"20 rounds of one id in between moved the counter by the same amount every time, so the move counts other clients' ids")
}

func TestSubscriptionIDMapper_RegisterAndGetMapping(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	routerID := "rs_abc123_00001"
	upstreamID := "0x1a2b3c4d"

	// Register mapping
	mapper.RegisterMapping(routerID, upstreamID)

	// Get upstream ID
	gotUpstream, found := mapper.GetUpstreamID(routerID)
	require.True(t, found)
	assert.Equal(t, upstreamID, gotUpstream)

	// Get router IDs
	routerIDs := mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 1)
	assert.Equal(t, routerID, routerIDs[0])
}

func TestSubscriptionIDMapper_MultipleRoutersToOneUpstream(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	upstreamID := "0x1a2b3c4d"
	routerID1 := "rs_client1_00001"
	routerID2 := "rs_client2_00001"
	routerID3 := "rs_client3_00001"

	// Multiple router IDs map to same upstream (subscription sharing)
	mapper.RegisterMapping(routerID1, upstreamID)
	mapper.RegisterMapping(routerID2, upstreamID)
	mapper.RegisterMapping(routerID3, upstreamID)

	// All should resolve to same upstream
	up1, _ := mapper.GetUpstreamID(routerID1)
	up2, _ := mapper.GetUpstreamID(routerID2)
	up3, _ := mapper.GetUpstreamID(routerID3)
	assert.Equal(t, upstreamID, up1)
	assert.Equal(t, upstreamID, up2)
	assert.Equal(t, upstreamID, up3)

	// Upstream should have all router IDs
	routerIDs := mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 3)
	assert.Contains(t, routerIDs, routerID1)
	assert.Contains(t, routerIDs, routerID2)
	assert.Contains(t, routerIDs, routerID3)
}

func TestSubscriptionIDMapper_RemoveMapping(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	upstreamID := "0x1a2b3c4d"
	routerID1 := "rs_client1_00001"
	routerID2 := "rs_client2_00001"

	mapper.RegisterMapping(routerID1, upstreamID)
	mapper.RegisterMapping(routerID2, upstreamID)

	// Remove first router - should not be last client
	gotUpstream, lastClient := mapper.RemoveMapping(routerID1)
	assert.Equal(t, upstreamID, gotUpstream)
	assert.False(t, lastClient, "should not be last client")

	// routerID1 should be gone
	_, found := mapper.GetUpstreamID(routerID1)
	assert.False(t, found)

	// routerID2 should still exist
	_, found = mapper.GetUpstreamID(routerID2)
	assert.True(t, found)

	// Upstream should have one router left
	routerIDs := mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 1)
	assert.Equal(t, routerID2, routerIDs[0])

	// Remove second router - should be last client
	gotUpstream, lastClient = mapper.RemoveMapping(routerID2)
	assert.Equal(t, upstreamID, gotUpstream)
	assert.True(t, lastClient, "should be last client")

	// Upstream should have no routers
	routerIDs = mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 0)
}

func TestSubscriptionIDMapper_RemoveMapping_NotFound(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	gotUpstream, lastClient := mapper.RemoveMapping("nonexistent")
	assert.Equal(t, "", gotUpstream)
	assert.False(t, lastClient)
}

func TestSubscriptionIDMapper_RemoveAllForUpstream(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	upstreamID := "0x1a2b3c4d"
	routerID1 := "rs_client1_00001"
	routerID2 := "rs_client2_00001"
	routerID3 := "rs_client3_00001"

	mapper.RegisterMapping(routerID1, upstreamID)
	mapper.RegisterMapping(routerID2, upstreamID)
	mapper.RegisterMapping(routerID3, upstreamID)

	// Remove all for upstream
	removed := mapper.RemoveAllForUpstream(upstreamID)
	assert.Len(t, removed, 3)
	assert.Contains(t, removed, routerID1)
	assert.Contains(t, removed, routerID2)
	assert.Contains(t, removed, routerID3)

	// All should be gone
	_, found := mapper.GetUpstreamID(routerID1)
	assert.False(t, found)
	_, found = mapper.GetUpstreamID(routerID2)
	assert.False(t, found)
	_, found = mapper.GetUpstreamID(routerID3)
	assert.False(t, found)

	// Upstream should have no routers
	routerIDs := mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 0)
}

func TestSubscriptionIDMapper_HasMethods(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	routerID := "rs_abc123_00001"
	upstreamID := "0x1a2b3c4d"

	assert.False(t, mapper.HasRouterID(routerID))
	assert.False(t, mapper.HasUpstreamID(upstreamID))

	mapper.RegisterMapping(routerID, upstreamID)

	assert.True(t, mapper.HasRouterID(routerID))
	assert.True(t, mapper.HasUpstreamID(upstreamID))
}

func TestSubscriptionIDMapper_Stats(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	routerCount, upstreamCount := mapper.Stats()
	assert.Equal(t, 0, routerCount)
	assert.Equal(t, 0, upstreamCount)

	// Add mappings
	mapper.RegisterMapping("rs_1", "up_a")
	mapper.RegisterMapping("rs_2", "up_a") // Same upstream
	mapper.RegisterMapping("rs_3", "up_b") // Different upstream

	routerCount, upstreamCount = mapper.Stats()
	assert.Equal(t, 3, routerCount)
	assert.Equal(t, 2, upstreamCount)
}

func TestSubscriptionIDMapper_ConcurrentAccess(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Concurrent registrations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				routerID := mapper.GenerateRouterID("client" + string(rune(clientNum)))
				mapper.RegisterMapping(routerID, "upstream_"+routerID)
			}
		}(i)
	}

	wg.Wait()

	// Verify integrity
	routerCount, upstreamCount := mapper.Stats()
	expectedCount := numGoroutines * numOperations
	assert.Equal(t, expectedCount, routerCount)
	assert.Equal(t, expectedCount, upstreamCount) // Each router has unique upstream in this test
}

func TestSubscriptionIDMapper_GetRouterIDs_ReturnsCopy(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	upstreamID := "0x1a2b3c4d"
	mapper.RegisterMapping("rs_1", upstreamID)
	mapper.RegisterMapping("rs_2", upstreamID)

	// Get router IDs
	ids1 := mapper.GetRouterIDs(upstreamID)

	// Modify the returned slice
	ids1[0] = "modified"

	// Get again - should not be affected
	ids2 := mapper.GetRouterIDs(upstreamID)
	assert.NotEqual(t, "modified", ids2[0], "returned slice should be a copy")
}

func TestSha256Short(t *testing.T) {
	hash1 := sha256Short("test")
	hash2 := sha256Short("test")
	hash3 := sha256Short("different")

	assert.Len(t, hash1, 6, "should return 6 characters")
	assert.Equal(t, hash1, hash2, "same input should produce same hash")
	assert.NotEqual(t, hash1, hash3, "different input should produce different hash")
}
