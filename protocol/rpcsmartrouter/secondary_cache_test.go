package rpcsmartrouter

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	ecocache "github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// fakeCacheReader is the CacheReader test seam: canned reply/error,
// optional context-respecting delay, and a recording of every GetEntry request so
// tests can assert what crossed the trust boundary (SharedStateId, blocks, ...).
type fakeCacheReader struct {
	active bool
	reply  *pairingtypes.CacheRelayReply
	err    error
	delay  time.Duration

	mu   sync.Mutex
	gets []*pairingtypes.RelayCacheGet
}

func (f *fakeCacheReader) CacheActive() bool { return f.active }

func (f *fakeCacheReader) GetEntry(ctx context.Context, relayCacheGet *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	f.mu.Lock()
	f.gets = append(f.gets, relayCacheGet)
	f.mu.Unlock()
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.reply, nil
}

func (f *fakeCacheReader) recorded() []*pairingtypes.RelayCacheGet {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pairingtypes.RelayCacheGet(nil), f.gets...)
}

// startCacheServerForTest runs a REAL cache server (ecosystem/cache) on a loopback
// gRPC listener and returns a connected performance.Cache client plus the server
// handle for direct GetRelay assertions. Backfill tests must go through the real
// server so its validation (SeenBlock expectations, TTL stores, compression) is
// exercised — a fake that merely records SetEntry cannot prove a follow-up GET hits.
func startCacheServerForTest(t *testing.T) (*performance.Cache, *ecocache.RelayerCacheServer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cs := &ecocache.CacheServer{CacheMaxCost: 1 << 20}
	cs.InitCache(ctx, time.Hour, 5*time.Second, 5*time.Second, time.Hour, "disabled", 1, 1)
	rcs := &ecocache.RelayerCacheServer{CacheServer: cs}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	pairingtypes.RegisterRelayerCacheServer(grpcServer, rcs)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	client, err := performance.InitCache(ctx, lis.Addr().String())
	require.NoError(t, err)
	require.True(t, client.CacheActive(), "test cache server must be connected")
	return client, rcs
}

// buildRestProtocolMessage builds a REAL protocol message (real chain parser, real
// HashCacheRequest) for a LATEST-block REST query, with a controlled
// RelayPrivateData.SeenBlock — the parse-time tip snapshot the backfill scenarios
// manipulate. The parser is returned too: tryCacheWriteResolved needs
// ChainBlockStats on the server under test.
func buildRestProtocolMessage(t *testing.T, ctx context.Context, seenBlock int64) (chainlib.ChainParser, chainlib.ProtocolMessage) {
	t.Helper()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/latest", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := &pairingtypes.RelayPrivateData{
		ConnectionType: http.MethodGet,
		ApiUrl:         "/cosmos/base/tendermint/v1beta1/blocks/latest",
		RequestBlock:   spectypes.LATEST_BLOCK,
		SeenBlock:      seenBlock,
		ApiInterface:   string(spectypes.APIInterfaceRest),
	}
	return chainParser, chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test-dapp", "127.0.0.1")
}

func newSecondaryTestServer(chainParser chainlib.ChainParser, primary *performance.Cache, secondary performance.CacheReader, timeout time.Duration) *RPCSmartRouterServer {
	return &RPCSmartRouterServer{
		cache:                 primary,
		secondaryCache:        secondary,
		secondaryCacheTimeout: timeout,
		chainParser:           chainParser,
		listenEndpoint:        &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: spectypes.APIInterfaceRest},
	}
}

// runSecondaryLookup drives trySecondaryCacheLookup with a live RelayProcessor
// reader (SetResponse blocks without one) and returns the served flag plus the
// processed result when one was served.
func runSecondaryLookup(t *testing.T, rpcss *RPCSmartRouterServer, protocolMessage chainlib.ProtocolMessage, requestedBlockForCache int64) (bool, *common.RelayResult) {
	t.Helper()
	ctx := context.Background()
	hashKey, outputFormatter, err := protocolMessage.HashCacheRequest("LAVA")
	require.NoError(t, err)

	usedProviders := lavasession.NewUsedProviders(nil)
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil}, protocolMessage, nil, false)
	require.NoError(t, err)
	relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

	waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
	defer waitCancel()
	waitDone := make(chan struct{})
	go func() { _ = relayProcessor.WaitForResults(waitCtx); close(waitDone) }()

	served := rpcss.trySecondaryCacheLookup(ctx, protocolMessage, protocolMessage.RelayPrivateData(), relayProcessor, nil, hashKey, outputFormatter, requestedBlockForCache)
	if !served {
		waitCancel()
		<-waitDone
		return false, nil
	}
	<-waitDone
	result, processingErr := relayProcessor.ProcessingResult()
	require.NoError(t, processingErr)
	require.NotNil(t, result)
	return true, result
}

func directGet(rcs *ecocache.RelayerCacheServer, hashKey []byte, block, seenBlock int64) *pairingtypes.CacheRelayReply {
	reply, _ := rcs.GetRelay(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash:    hashKey,
		ChainId:        "LAVA",
		RequestedBlock: block,
		SeenBlock:      seenBlock,
		Finalized:      false,
	})
	return reply
}

// ---------------------------------------------------------------------------
// Behavioral tests
// ---------------------------------------------------------------------------

// A secondary hit serves the caller with no primary configured at all —
// and the request-side contract holds: exactly one lookup, SharedStateId empty
// (never join a foreign fleet's shared state), Finalized=false, the resolved block.
func TestSecondaryCacheHitServesWithoutPrimary(t *testing.T) {
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply:     &pairingtypes.RelayReply{Data: []byte(`{"block":{"header":{"height":"100"}}}`), LatestBlock: 100},
			SeenBlock: 100,
		},
	}
	rpcss := newSecondaryTestServer(chainParser, nil, fake, 100*time.Millisecond)

	served, result := runSecondaryLookup(t, rpcss, protocolMessage, 100)

	require.True(t, served)
	require.Contains(t, string(result.Reply.Data), `"height":"100"`)
	require.Equal(t, 200, result.StatusCode, "legacy zero status is served as assumed-success 200")
	require.False(t, result.IsNodeError)
	require.Equal(t, "", result.ProviderInfo.ProviderAddress, "must render as Cached downstream")

	gets := fake.recorded()
	require.Len(t, gets, 1)
	require.Equal(t, "", gets[0].SharedStateId, "secondary GET must never carry SharedStateId")
	require.False(t, gets[0].Finalized)
	require.Equal(t, int64(100), gets[0].RequestedBlock)
}

// Every non-hit degrades to the same fall-through, one per outcome in
// ClassifyCacheLookupOutcome: a clean not-found, a transport error, and an exceeded
// budget. The tier can never fail or stall a request.
func TestSecondaryCacheTimeoutAndErrorAreMisses(t *testing.T) {
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)

	// The plain not-found: no error, no reply. Distinct from the failure cases
	// because the cache answered correctly — and its answer must still leave nothing
	// behind. Block-hash→height mappings are chain-scoped state that steers
	// resolveRequestedBlock and archive routing; a foreign tier does not get to supply
	// them, so the lookup neither asks for them nor hands the reply back.
	t.Run("clean not-found", func(t *testing.T) {
		fake := &fakeCacheReader{
			active: true,
			reply: &pairingtypes.CacheRelayReply{
				Reply:                 nil,
				BlocksHashesToHeights: []*pairingtypes.BlockHashToHeight{{Hash: "0xabc", Height: 90}},
			},
		}
		rpcss := newSecondaryTestServer(chainParser, nil, fake, 100*time.Millisecond)
		hashKey, outputFormatter, err := protocolMessage.HashCacheRequest("LAVA")
		require.NoError(t, err)
		served := rpcss.trySecondaryCacheLookup(context.Background(), protocolMessage,
			protocolMessage.RelayPrivateData(), nil, nil, hashKey, outputFormatter, 100)
		require.False(t, served, "a not-found must fall through to providers")

		gets := fake.recorded()
		require.Len(t, gets, 1)
		require.Nil(t, gets[0].BlocksHashesToHeights,
			"the secondary GET must not request hash→height mappings it is not allowed to use")
	})

	t.Run("timeout", func(t *testing.T) {
		fake := &fakeCacheReader{active: true, delay: 500 * time.Millisecond, reply: &pairingtypes.CacheRelayReply{Reply: &pairingtypes.RelayReply{Data: []byte("late")}}}
		rpcss := newSecondaryTestServer(chainParser, nil, fake, 30*time.Millisecond)
		start := time.Now()
		served, _ := runSecondaryLookup(t, rpcss, protocolMessage, 100)
		require.False(t, served)
		require.Less(t, time.Since(start), 300*time.Millisecond, "lookup must be bounded by secondary-cache-timeout")
	})

	t.Run("error", func(t *testing.T) {
		fake := &fakeCacheReader{active: true, err: errors.New("connection reset")}
		rpcss := newSecondaryTestServer(chainParser, nil, fake, 100*time.Millisecond)
		served, _ := runSecondaryLookup(t, rpcss, protocolMessage, 100)
		require.False(t, served)
	})
}

// The exact-key backfill blocker regression, through REAL cache-server
// validation. Scenario: ParseRelay stamped SeenBlock=99, the guarded tip advanced
// to 100 by lookup time (requestedBlockForCache=100), and the cached entry has
// Reply.LatestBlock=0 (an eth_call-style reply with no parsable height). The
// backfill must land at key 100 with a validity SeenBlock lifted to 100 — the
// follow-up primary GET with RequestedBlock=100/SeenBlock=100 must return it.
// Without the lift the server stores SeenBlock=99 and rejects that GET as
// "reply seen block is smaller than our expectations".
func TestSecondaryHitBackfillsPrimaryWithExactKeyAndValidSeenBlock(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 99)
	payload := []byte(`{"block":{"header":{"height":"100"}}}`)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply:     &pairingtypes.RelayReply{Data: payload, LatestBlock: 0},
			SeenBlock: 100,
		},
	}
	rpcss := newSecondaryTestServer(chainParser, primary, fake, 100*time.Millisecond)

	hashKey, _, err := protocolMessage.HashCacheRequest("LAVA")
	require.NoError(t, err)
	served, _ := runSecondaryLookup(t, rpcss, protocolMessage, 100)
	require.True(t, served)

	require.Eventually(t, func() bool {
		reply := directGet(rcs, hashKey, 100, 100)
		return reply.GetReply() != nil
	}, 3*time.Second, 25*time.Millisecond, "backfilled entry must survive the primary's SeenBlock validation at (block=100, seen=100)")

	reply := directGet(rcs, hashKey, 100, 100)
	require.NotNil(t, reply.GetReply())
	require.Equal(t, payload, reply.GetReply().Data)
	require.GreaterOrEqual(t, reply.GetSeenBlock(), int64(100), "stored validity floor must be lifted to the locally resolved block")
	require.Equal(t, 0, reply.GetStatusCode(),
		"the entry's writer recorded no status, so the backfill must store zero (unknown) — "+
			"stamping the assumed-200 used for serving would launder unknown into observed")
	require.False(t, reply.GetIsNodeError())
}

// A status the entry's writer DID record survives the backfill verbatim, and the
// caller is served that status rather than the assumed 200. Companion to the
// zero-status case above: together they pin that the served default never leaks into
// the store, and that a real status is never masked.
func TestSecondaryHitPreservesWriterRecordedStatusThroughBackfill(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)
	payload := []byte(`{"block":{"header":{"height":"100"}}}`)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply:      &pairingtypes.RelayReply{Data: payload, LatestBlock: 100},
			SeenBlock:  100,
			StatusCode: 203, // a 2xx the populator accepts, distinguishable from the 200 default
		},
	}
	rpcss := newSecondaryTestServer(chainParser, primary, fake, 100*time.Millisecond)

	hashKey, _, err := protocolMessage.HashCacheRequest("LAVA")
	require.NoError(t, err)
	served, result := runSecondaryLookup(t, rpcss, protocolMessage, 100)
	require.True(t, served)
	require.Equal(t, 203, result.StatusCode, "a recorded status must reach the caller unmasked")

	require.Eventually(t, func() bool {
		return directGet(rcs, hashKey, 100, 100).GetReply() != nil
	}, 3*time.Second, 25*time.Millisecond)
	require.Equal(t, 203, directGet(rcs, hashKey, 100, 100).GetStatusCode(), "recorded status must round-trip through the backfill")
}

// A status the populator rejects (429) is served to the caller but never backfilled —
// the entry's real status reaching RelayResult is what makes the populator's
// status-code check reachable at all.
func TestSecondaryHitWithRejectedStatusServesButNeverBackfills(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"error":"rate limited"}`), LatestBlock: 100},
			SeenBlock:  100,
			StatusCode: http.StatusTooManyRequests,
		},
	}
	rpcss := newSecondaryTestServer(chainParser, primary, fake, 100*time.Millisecond)

	hashKey, _, err := protocolMessage.HashCacheRequest("LAVA")
	require.NoError(t, err)
	served, result := runSecondaryLookup(t, rpcss, protocolMessage, 100)
	require.True(t, served)
	require.Equal(t, http.StatusTooManyRequests, result.StatusCode)

	time.Sleep(700 * time.Millisecond)
	require.Nil(t, directGet(rcs, hashKey, 100, 100).GetReply(), "a 429 entry must never be backfilled into the primary")
}

// Cached node errors — explicit flag and legacy placeholder — are served but
// NEVER backfilled, and the rejection is the populator's own node-error check (the
// RelayResult carries IsNodeError; there is no pre-filter in the secondary path).
func TestSecondaryCachedNodeErrorsServeButNeverBackfill(t *testing.T) {
	cases := []struct {
		name  string
		reply *pairingtypes.CacheRelayReply
	}{
		{
			name: "explicit IsNodeError flag",
			reply: &pairingtypes.CacheRelayReply{
				Reply:       &pairingtypes.RelayReply{Data: []byte(`{"error":"execution reverted"}`), LatestBlock: 100},
				SeenBlock:   100,
				IsNodeError: true,
			},
		},
		{
			name: "legacy CACHED_ERROR placeholder",
			reply: &pairingtypes.CacheRelayReply{
				Reply:     &pairingtypes.RelayReply{Data: []byte(`{"Error_GUID":"CACHED_ERROR","error":"execution reverted"}`), LatestBlock: 100},
				SeenBlock: 100,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary, rcs := startCacheServerForTest(t)
			chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)
			fake := &fakeCacheReader{active: true, reply: tc.reply}
			rpcss := newSecondaryTestServer(chainParser, primary, fake, 100*time.Millisecond)

			hashKey, _, err := protocolMessage.HashCacheRequest("LAVA")
			require.NoError(t, err)
			served, result := runSecondaryLookup(t, rpcss, protocolMessage, 100)
			require.True(t, served, "node errors are served (with GUID substitution), only backfill is rejected")
			require.True(t, result.IsNodeError, "entry kind must reach the RelayResult so the populator can see it")

			// The write is async; give it ample time to happen if the rejection were
			// broken, then prove the primary stayed empty.
			time.Sleep(700 * time.Millisecond)
			reply := directGet(rcs, hashKey, 100, 100)
			require.Nil(t, reply.GetReply(), "a cached node error must never be backfilled into the primary")
		})
	}
}

// Full-path poisoned-entry test: foreign signatures, ARBITRARY foreign metadata
// (names no denylist could enumerate) and a foreign chain height appear neither in
// the served result nor in the primary backfill, while the transport headers that
// describe how to decode the payload survive — the allowlist sanitization policy.
func TestSecondaryPoisonedEntrySanitizedForCallerAndBackfill(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)
	payload := []byte(`{"block":{"header":{"height":"100"}}}`)
	// A foreign zone's chain head, wildly ahead of this router's. Unlike the
	// identity fields this one is not merely embarrassing: on the backfill path
	// SetRelay publishes Response.LatestBlock as the cache server's chain-level tip
	// through a monotonic-max write, and that key is what LATEST/SAFE/FINALIZED/
	// PENDING resolve to for the whole chain until it expires.
	const foreignHead = int64(987654321)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply: &pairingtypes.RelayReply{
				Data:        payload,
				LatestBlock: foreignHead,
				Sig:         []byte{0xde, 0xad},
				SigBlocks:   []byte{0xbe, 0xef},
				Metadata: []pairingtypes.Metadata{
					{Name: "Lava-Provider-Address", Value: "lava@provider1"},
					{Name: "X-Provider-ID", Value: "prov-42"},
					{Name: "X-Backend", Value: "geth-fra-03"},
					{Name: "X-Served-By", Value: "edge-9.internal"},
					{Name: "Via", Value: "1.1 lb.provider.example"},
					{Name: "Server", Value: "nginx/1.25 (provider-pool-b)"},
					// the one header that must NOT go: Reply.Metadata is the only source
					// of client response headers, so dropping it retypes the body
					{Name: "Content-Type", Value: "application/json"},
				},
			},
			SeenBlock: 100,
			// foreign OptionalMetadata is ignored wholesale by the secondary path
			OptionalMetadata: []pairingtypes.Metadata{{Name: "X-Origin-Provider", Value: "prov-42"}},
		},
	}
	rpcss := newSecondaryTestServer(chainParser, primary, fake, 100*time.Millisecond)

	hashKey, _, err := protocolMessage.HashCacheRequest("LAVA")
	require.NoError(t, err)
	served, result := runSecondaryLookup(t, rpcss, protocolMessage, 100)
	require.True(t, served)

	// caller-visible result: no foreign identity anywhere, transport headers intact
	require.Nil(t, result.Reply.Sig)
	require.Nil(t, result.Reply.SigBlocks)
	require.Equal(t, []pairingtypes.Metadata{{Name: "Content-Type", Value: "application/json"}}, result.Reply.Metadata,
		"every identity-bearing header is dropped; only the transport allowlist survives")
	require.Equal(t, "", result.ProviderInfo.ProviderAddress, "locally minted Lava-Provider-Address: Cached still applies downstream")
	require.Equal(t, payload, result.Reply.Data)
	require.Zero(t, result.Reply.LatestBlock, "a foreign chain height must not ride out on the served reply")

	// backfill payload: the sanitized clone is the only thing written
	require.Eventually(t, func() bool {
		return directGet(rcs, hashKey, 100, 100).GetReply() != nil
	}, 3*time.Second, 25*time.Millisecond)
	stored := directGet(rcs, hashKey, 100, 100)
	require.Equal(t, []pairingtypes.Metadata{{Name: "Content-Type", Value: "application/json"}}, stored.GetReply().Metadata,
		"foreign identity metadata must not reach the primary store, but the entry must stay decodable from it")
	require.Empty(t, stored.GetReply().Sig)
	require.Empty(t, stored.GetReply().SigBlocks)
	require.Equal(t, payload, stored.GetReply().Data)
	require.Zero(t, stored.GetReply().LatestBlock, "a foreign chain height must not be persisted in the primary store")

	// The consequence that actually matters: the foreign head must not have become
	// the primary's chain-level tip. A LATEST-tagged lookup resolves against that
	// tip, so if the foreign value had been published, this would resolve to block
	// 987654321 — a key nobody ever wrote — and miss. Resolving to the locally
	// derived 100 and hitting is the proof it was not.
	byTag := directGet(rcs, hashKey, spectypes.LATEST_BLOCK, 0)
	require.NotNil(t, byTag.GetReply(), "LATEST must still resolve to this router's own tip, not the foreign head")
	require.Equal(t, payload, byTag.GetReply().Data)
}

// The secondary reply's SeenBlock is never adopted into ChainState, even with
// shared-state mode on — shared-state tip exchange is fleet-scoped and a foreign
// cache is not this router's fleet.
func TestSecondarySeenBlockNeverAdoptedIntoChainState(t *testing.T) {
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply:     &pairingtypes.RelayReply{Data: []byte(`{"block":{"header":{"height":"100"}}}`), LatestBlock: 100},
			SeenBlock: 987654, // a foreign "tip" that must go nowhere
		},
	}
	rpcss := newSecondaryTestServer(chainParser, nil, fake, 100*time.Millisecond)
	rpcss.sharedState = true
	rpcss.chainState = chainstate.New("LAVA", chainstate.DefaultConfig(12*time.Second))

	served, _ := runSecondaryLookup(t, rpcss, protocolMessage, 100)
	require.True(t, served)

	_, ok := rpcss.chainState.GetLatestBlock()
	require.False(t, ok, "foreign SeenBlock must never reach ChainState")
}

// ---------------------------------------------------------------------------
// Configuration wiring — through the real cobra command's flags
// and the same viper mechanics the RunE uses (BindPFlags + optional YAML config).
// Environment variables are intentionally NOT bound anywhere in this repo, which is
// why configuration is scoped to flags and YAML.
// ---------------------------------------------------------------------------

func TestSecondaryCacheFlagAndYamlWiring(t *testing.T) {
	t.Run("flags bind and IsSet distinguishes explicit values from defaults", func(t *testing.T) {
		cmd := CreateRPCSmartRouterCobraCommand()
		require.NoError(t, cmd.ParseFlags([]string{
			"--secondary-cache-be", "cache-shared:20100",
			"--secondary-cache-timeout", "75ms",
		}))
		v := viper.New()
		require.NoError(t, v.BindPFlags(cmd.Flags()))

		require.Equal(t, "cache-shared:20100", v.GetString(performance.SecondaryCacheFlagName))
		require.Equal(t, 75*time.Millisecond, v.GetDuration(performance.SecondaryCacheTimeoutFlagName))
		require.Equal(t, performance.SecondaryCacheModeReadOnly, v.GetString(performance.SecondaryCacheModeFlagName), "mode default")
		require.True(t, v.IsSet(performance.SecondaryCacheTimeoutFlagName), "explicit flag must count as set (dangling-config validation input)")
		require.False(t, v.IsSet(performance.SecondaryCacheModeFlagName), "untouched default must not count as set")
	})

	t.Run("yaml supplies values and a changed flag wins precedence", func(t *testing.T) {
		cmd := CreateRPCSmartRouterCobraCommand()
		require.NoError(t, cmd.ParseFlags([]string{"--secondary-cache-timeout", "99ms"}))
		v := viper.New()
		require.NoError(t, v.BindPFlags(cmd.Flags()))
		v.SetConfigType("yml")
		require.NoError(t, v.ReadConfig(strings.NewReader("secondary-cache-be: yaml-cache:20100\nsecondary-cache-timeout: 33ms\n")))

		require.Equal(t, "yaml-cache:20100", v.GetString(performance.SecondaryCacheFlagName), "YAML must supply the address")
		require.Equal(t, 99*time.Millisecond, v.GetDuration(performance.SecondaryCacheTimeoutFlagName), "explicitly changed flag must beat YAML")
		require.True(t, v.IsSet(performance.SecondaryCacheFlagName), "YAML-provided key counts as set")
	})
}

// buildHashBearingProtocolMessage builds a REAL protocol message whose
// GetRequestedBlocksHashes is non-empty — the shape the primary tier feeds into
// RelayCacheGet.BlocksHashesToHeights. ETH1/`trace_transaction` is used because it is
// the bundled spec's hash-parsed method (`parse_type: BLOCK_HASH` on `.params.[0]`);
// eth_getBlockByHash carries no such parser and yields no hashes, which would make a
// "we never ask" assertion vacuously true.
func buildHashBearingProtocolMessage(t *testing.T, ctx context.Context, blockHash string) (chainlib.ChainParser, chainlib.ProtocolMessage) {
	t.Helper()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"trace_transaction","params":["` + blockHash + `"]}`)
	chainMsg, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := &pairingtypes.RelayPrivateData{
		ConnectionType: http.MethodPost,
		Data:           body,
		SeenBlock:      100,
		ApiInterface:   string(spectypes.APIInterfaceJsonRPC),
	}
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test-dapp", "127.0.0.1")
	require.Equal(t, []string{blockHash}, protocolMessage.GetRequestedBlocksHashes(),
		"fixture must genuinely carry a requested block hash, or the assertion below proves nothing")
	return chainParser, protocolMessage
}

// The second half of the foreign-chain-state class the LatestBlock fix closed.
// BlocksHashesToHeights lives on the OUTER CacheRelayReply, structurally out of
// SanitizeForeignCacheReply's reach, and the heights it carries steer two local
// decisions: resolveRequestedBlock raises reqBlock to the latest of them (gating
// endpoint sync and optimizer selection) and the earliest drives
// UpdateEarliestAndValidateExtensionRules into archive routing. Folded in, the
// max-for-latest rule below would raise reqBlock to 987654321 and strand the request
// on "no endpoint is synced to that block".
//
// The lookup therefore never asks for them — pinned on the REQUEST rather than on the
// reply, because a request that does not ask cannot be re-plumbed into a fold by a
// later change without this failing first. The request carries a real hash, so the
// primary tier's own call site would populate the field here.
func TestSecondaryLookupNeverRequestsForeignBlockHashHeights(t *testing.T) {
	const blockHash = "0x1111111111111111111111111111111111111111111111111111111111111111"
	chainParser, protocolMessage := buildHashBearingProtocolMessage(t, context.Background(), blockHash)

	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			// A foreign zone claiming an absurd height for the hash in play.
			BlocksHashesToHeights: []*pairingtypes.BlockHashToHeight{{Hash: blockHash, Height: 987654321}},
		},
	}
	rpcss := newSecondaryTestServer(chainParser, nil, fake, 100*time.Millisecond)
	rpcss.listenEndpoint = &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}

	hashKey, outputFormatter, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)
	served := rpcss.trySecondaryCacheLookup(context.Background(), protocolMessage,
		protocolMessage.RelayPrivateData(), nil, nil, hashKey, outputFormatter, 100)
	require.False(t, served, "reply-less entry is a miss")

	gets := fake.recorded()
	require.Len(t, gets, 1)
	require.Nil(t, gets[0].BlocksHashesToHeights,
		"the secondary GET must not ask for hash→height mappings this router is not allowed to adopt")
}

// Zeroing LatestBlock in the sanitizer is not quite inert on the serving path: its one
// remaining reader is appendHeadersToRelayResult, which emits Provider-Latest-Block
// whenever the value is positive. Left at zero, a secondary hit would be the single
// response in the system that omits that header, so the two tiers would not be
// interchangeable to a caller — which is exactly what operators are told they are.
// The re-stamped value must be the LOCAL gated tip and never the foreign head.
func TestSecondaryHitRestampsLatestBlockFromLocalTip(t *testing.T) {
	const (
		foreignHead = int64(987654321)
		localTip    = int64(100)
	)
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), localTip)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply:     &pairingtypes.RelayReply{Data: []byte(`{"block":{"header":{"height":"100"}}}`), LatestBlock: foreignHead},
			SeenBlock: localTip,
		},
	}
	rpcss := newSecondaryTestServer(chainParser, nil, fake, 100*time.Millisecond)
	rpcss.chainState = chainstate.New("LAVA", chainstate.DefaultConfig(12*time.Second))
	rpcss.chainState.SetLatestBlock(localTip)

	served, result := runSecondaryLookup(t, rpcss, protocolMessage, localTip)
	require.True(t, served)
	require.Equal(t, localTip, result.Reply.LatestBlock,
		"Provider-Latest-Block must be served from this router's own tip, so both tiers emit it")
	require.NotEqual(t, foreignHead, result.Reply.LatestBlock)
}

// A router that has not observed a tip yet leaves the field at zero and simply omits
// the header, exactly as it already does on any other path with no tip — the re-stamp
// must not invent a height, least of all the foreign one it just dropped.
func TestSecondaryHitOmitsLatestBlockWithNoLocalTip(t *testing.T) {
	chainParser, protocolMessage := buildRestProtocolMessage(t, context.Background(), 100)
	fake := &fakeCacheReader{
		active: true,
		reply: &pairingtypes.CacheRelayReply{
			Reply: &pairingtypes.RelayReply{Data: []byte(`{"block":{"header":{"height":"100"}}}`), LatestBlock: 987654321},
		},
	}
	rpcss := newSecondaryTestServer(chainParser, nil, fake, 100*time.Millisecond)

	served, result := runSecondaryLookup(t, rpcss, protocolMessage, 100)
	require.True(t, served)
	require.Zero(t, result.Reply.LatestBlock, "no local tip means no header, not a foreign one")
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

// Entry-kind resolution is the single helper BOTH tiers call, which is what makes a
// replayed node error carry lava-identified-node-error regardless of which cache
// answered. Pinning it here covers the primary tier's use too: that call site is two
// lines inside sendRelayToEndpoint, which cannot be driven without a full session and
// endpoint fixture, whereas the decision itself lives entirely in this function.
func TestResolveCachedEntryKind(t *testing.T) {
	const guid = uint64(4242)
	guidCtx := utils.WithUniqueIdentifier(context.Background(), guid)
	placeholder := []byte(`{"Error_GUID":"CACHED_ERROR","error":"execution reverted"}`)

	t.Run("explicit flag marks a node error and leaves the payload alone", func(t *testing.T) {
		data := []byte(`{"error":"execution reverted"}`)
		isNodeError, resolved := resolveCachedEntryKind(guidCtx, &pairingtypes.CacheRelayReply{IsNodeError: true}, data)
		require.True(t, isNodeError)
		require.Equal(t, data, resolved, "no placeholder means no substitution")
	})

	t.Run("legacy placeholder implies node error and is substituted", func(t *testing.T) {
		isNodeError, resolved := resolveCachedEntryKind(guidCtx, &pairingtypes.CacheRelayReply{}, placeholder)
		require.True(t, isNodeError, "the placeholder is the entry-kind signal for backends predating IsNodeError")
		require.Contains(t, string(resolved), `"Error_GUID":"4242"`, "the placeholder must carry this request's GUID")
		require.NotContains(t, string(resolved), "CACHED_ERROR")
	})

	t.Run("a plain success is neither flagged nor rewritten", func(t *testing.T) {
		data := []byte(`{"result":"0x64"}`)
		isNodeError, resolved := resolveCachedEntryKind(guidCtx, &pairingtypes.CacheRelayReply{}, data)
		require.False(t, isNodeError)
		require.Equal(t, data, resolved)
	})

	t.Run("kind survives a context with no GUID", func(t *testing.T) {
		isNodeError, resolved := resolveCachedEntryKind(context.Background(), &pairingtypes.CacheRelayReply{}, placeholder)
		require.True(t, isNodeError, "kind is decided before substitution, so a missing GUID cannot hide it")
		require.Equal(t, placeholder, resolved, "without a GUID the placeholder is left intact rather than half-rewritten")
	})

	t.Run("nil reply is safe and reports a success", func(t *testing.T) {
		data := []byte(`{"result":"0x64"}`)
		isNodeError, resolved := resolveCachedEntryKind(guidCtx, nil, data)
		require.False(t, isNodeError)
		require.Equal(t, data, resolved)
	})
}

// The secondary tier must be skipped cleanly in every unconfigured/disconnected
// state: nil interface, and an interface wrapping a typed-nil concrete
// client (the wiring hands over a nil *performance.Cache when disabled).
func TestSecondaryCacheActiveNilSafety(t *testing.T) {
	rpcss := &RPCSmartRouterServer{}
	require.False(t, rpcss.secondaryCacheActive(), "nil interface must be inactive")

	rpcss.secondaryCache = (*performance.Cache)(nil)
	require.False(t, rpcss.secondaryCacheActive(), "typed-nil concrete client must be inactive")
}
