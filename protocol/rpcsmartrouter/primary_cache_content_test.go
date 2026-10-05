package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// MAG-3597, the primary tier. A primary shared by a fleet mid-rollout holds entries an
// older build wrote: an error body with no label, or the bytes it made from a web page.
// The primary lookup judges them the way the secondary does. That lookup lives inline
// in sendRelayToEndpoint, which no test drives end to end, so these stage the entry in
// a real primary cache server, read it back through the same client the router uses,
// and put the reply through the two steps that path runs on it: primaryLookupVerdict on
// the raw reply, classifyCachedEntry on the formatted one.
func TestPrimaryEntryIsJudgedByItsContents(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := secondaryEthParser(t)
	protocolMessage := secondaryEthMessage(t, chainParser, secondaryEthRequest, 100)
	hashKey, outputFormatter, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)

	// stage writes an entry the way an older build did: bytes and a block, no label.
	stage := func(t *testing.T, block int64, data string) *pairingtypes.CacheRelayReply {
		t.Helper()
		_, err := rcs.SetRelay(context.Background(), &pairingtypes.RelayCacheSet{
			RequestHash:      hashKey,
			ChainId:          "ETH1",
			RequestedBlock:   block,
			SeenBlock:        block,
			Response:         &pairingtypes.RelayReply{Data: []byte(data), LatestBlock: block},
			Finalized:        false,
			AverageBlockTime: int64(15 * time.Second),
			StatusCode:       http.StatusOK,
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return directGetETH(rcs, hashKey, block, block).GetReply() != nil
		}, 3*time.Second, 25*time.Millisecond, "the staged entry is readable on the server")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		cacheReply, err := primary.GetEntry(ctx, &pairingtypes.RelayCacheGet{RequestHash: hashKey, ChainId: "ETH1", RequestedBlock: block, SeenBlock: block})
		require.NoError(t, err)
		require.NotNil(t, cacheReply.GetReply(), "the router's own client sees the entry")
		return cacheReply
	}

	t.Run("an error body with no label is served as a node error", func(t *testing.T) {
		cacheReply := stage(t, 100, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}`)
		hit, unusable, outcome := primaryLookupVerdict(nil, cacheReply, spectypes.APIInterfaceJsonRPC)
		require.True(t, hit)
		require.False(t, unusable)
		require.Equal(t, metrics.CacheOutcomeHit, outcome)
		require.False(t, cacheReply.GetIsNodeError(), "the writer attached no label")

		isNodeError, resolved := classifyCachedEntry(context.Background(), protocolMessage, metrics.CacheTierPrimary, cacheReply, outputFormatter(cacheReply.GetReply().Data))
		require.True(t, isNodeError, "the contents decide, so the header follows")
		require.Equal(t, `7`, gjson.GetBytes(resolved, "id").Raw, "the caller's id is restored on the error too")
	})

	t.Run("a success body is served as a success", func(t *testing.T) {
		cacheReply := stage(t, 101, `{"jsonrpc":"2.0","id":1,"result":"0x64"}`)
		hit, unusable, outcome := primaryLookupVerdict(nil, cacheReply, spectypes.APIInterfaceJsonRPC)
		require.True(t, hit)
		require.False(t, unusable)
		require.Equal(t, metrics.CacheOutcomeHit, outcome)

		isNodeError, resolved := classifyCachedEntry(context.Background(), protocolMessage, metrics.CacheTierPrimary, cacheReply, outputFormatter(cacheReply.GetReply().Data))
		require.False(t, isNodeError)
		require.Equal(t, `7`, gjson.GetBytes(resolved, "id").Raw)
		require.Equal(t, `"0x64"`, gjson.GetBytes(resolved, "result").Raw)
	})

	t.Run("an entry that is not a reply is no hit and is recorded as an error", func(t *testing.T) {
		for block, data := range map[int64]string{
			102: `<html>502 Bad Gateway</html>`,
			103: `{"jsonrpc":"2.0","id":1}`,
		} {
			cacheReply := stage(t, block, data)
			hit, unusable, outcome := primaryLookupVerdict(nil, cacheReply, spectypes.APIInterfaceJsonRPC)
			require.False(t, hit, "%q is not served as a reply", data)
			require.True(t, unusable)
			require.Equal(t, metrics.CacheOutcomeError, outcome, "the tier answered, this router refused it: an error, not a miss")
		}
	})
}

// The verdict on the answers the primary lookup gets that do not involve an entry:
// nothing found, and the client failing. Both are as before; only a hit is judged.
func TestPrimaryLookupVerdictOnNonHits(t *testing.T) {
	hit, unusable, outcome := primaryLookupVerdict(nil, &pairingtypes.CacheRelayReply{}, spectypes.APIInterfaceJsonRPC)
	require.False(t, hit)
	require.False(t, unusable)
	require.Equal(t, metrics.CacheOutcomeMiss, outcome)

	hit, unusable, outcome = primaryLookupVerdict(context.DeadlineExceeded, nil, spectypes.APIInterfaceJsonRPC)
	require.False(t, hit)
	require.False(t, unusable)
	require.Equal(t, metrics.CacheOutcomeTimeout, outcome)

	// REST carries no envelope to judge: a body that is not JSON is still a hit there.
	hit, unusable, outcome = primaryLookupVerdict(nil, &pairingtypes.CacheRelayReply{Reply: &pairingtypes.RelayReply{Data: []byte(`<html>`)}}, spectypes.APIInterfaceRest)
	require.True(t, hit)
	require.False(t, unusable)
	require.Equal(t, metrics.CacheOutcomeHit, outcome)
}
