package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// craftGetBlockMessage builds the GRPCTEST get-block request the way a relay would:
// through the spec's own parse directive, so the message is genuinely sendable
// rather than a hand-assembled stand-in.
func craftGetBlockMessage(t *testing.T, parser chainlib.ChainParser) chainlib.ChainMessage {
	t.Helper()
	parsing, apiCollection, ok := parser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok, "GRPCTEST must declare a get-blocknum directive")

	message, err := parser.ParseMsg(parsing.ApiName, []byte{},
		apiCollection.CollectionData.Type, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	return message
}

// boundRegistry reports which registry a parser is currently bound to, through
// exported API only: ParseMsg copies apip.registry onto every message it builds, and
// that registry is what GetParams -> block parsing resolves through. It is the same
// pointer live relays use, so comparing it across an event answers "did that event
// rebind the live parser".
func boundRegistry(t *testing.T, message chainlib.ChainMessage) *dyncodec.Registry {
	t.Helper()
	grpcMessage, ok := message.GetRPCMessage().(*rpcInterfaceMessages.GrpcMessage)
	require.True(t, ok, "GRPCTEST messages must be gRPC messages")
	return grpcMessage.Registry
}

// TestRevalidateTier_DoesNotRebindLiveGrpcParser is MAG-2538's regression pin.
//
// Background provider recovery used to hand the live chainParser straight to
// GetChainRouter under a per-attempt timeout. For gRPC, newGrpcChainProxy ->
// setupForProvider rebinds that parser's registry/codec to the verification
// connection, so when the attempt context was cancelled the live parser was left
// resolving descriptors through a dead reflection connection: every subsequent
// gRPC relay on that provider failed in dynamicResolve with "proto file containing
// symbol: ..." until the pod restarted. The retry runs on a timer, so the outage
// recurred every few minutes.
//
// The ticket's Expected clause is "a recovering provider is validated against a
// copy of the parser, not the live one", and that is what this asserts — against
// the real recovery path, not a stand-in. revalidateTier is the production
// function; it calls the real retryValidateFn -> validateProvider, which is where
// CloneChainParserForValidation is applied. Deleting that one call fails this test.
//
// TestCloneChainParserForValidation_GrpcIsolation in chainlib covers the helper in
// isolation, but simulates the mutation by assigning registry/codec directly. It
// therefore cannot see a caller that stops cloning, which is exactly the defect
// this ticket was filed for.
//
// This is NOT the end-to-end repro the ticket also asks for. It does not drive the
// 3-minute retry timer and it does not serve a relay through external gRPC ingress;
// both remain blocked on MAG-2477 / MAG-2093. What it does cover is the mutation
// that caused the outage, on the path that performs it, asserted both as a binding
// and as the descriptor resolution that binding feeds.
func TestRevalidateTier_DoesNotRebindLiveGrpcParser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// CreateChainLibMocks stands up a gRPC server that serves both reflection and
	// the GRPCTEST block service, then builds the parser through GetChainRouter —
	// i.e. the parser arrives already bound the way boot leaves it in production.
	// The router it returns is the SERVING router, the one live relays would use.
	noop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	chainParser, servingRouter, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, "GRPCTEST", spectypes.APIInterfaceGrpc, noop, nil, "../../", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeServer != nil {
			closeServer()
		}
	})

	servingRegistry := boundRegistry(t, craftGetBlockMessage(t, chainParser))
	require.NotNil(t, servingRegistry,
		"precondition: boot must leave the live parser bound, or there is nothing for recovery to break")

	provider := &routersession.RPCStaticProviderEndpoint{
		Name:         "recovering-provider",
		ChainID:      endpoint.ChainID,
		ApiInterface: endpoint.ApiInterface,
		NodeUrls:     endpoint.NodeUrls,
	}
	rpcEndpoint := &routersession.RPCEndpoint{
		ChainID:      endpoint.ChainID,
		ApiInterface: endpoint.ApiInterface,
	}

	// The production recovery pass, verbatim: revalidateTier -> retryValidateFn ->
	// validateProvider -> CloneChainParserForValidation -> GetChainRouter, with
	// validateProvider's own attemptCtx cancelled on the way out.
	recovered, stillFailed := (&RPCSmartRouter{}).revalidateTier(
		ctx,
		[]*routersession.RPCStaticProviderEndpoint{provider},
		chainParser,
		rpcEndpoint,
		reverifyTierStatic,
	)

	// Verification has to have reached the mutation for the assertions below to mean
	// anything: if GetChainRouter had failed early, setupForProvider would never run
	// and the live parser would be untouched for the wrong reason. A recovered
	// provider proves the router AND the fetcher were built against this endpoint.
	require.Empty(t, stillFailed, "the mock upstream should verify; otherwise this test proves nothing")
	require.Len(t, recovered, 1)

	require.Same(t, servingRegistry, boundRegistry(t, craftGetBlockMessage(t, chainParser)),
		"MAG-2538: background recovery must validate against a clone, leaving the live parser bound to the serving connection")

	// The same binding as above, asserted through behaviour instead of pointer
	// identity. GetParams -> dynamicResolve is the ONLY consumer of the parser's
	// registry, and it is what block parsing runs through; against a registry left
	// bound to recovery's torn-down reflection connection it logs "failed to
	// dynamicResolve" and returns nil. Worth keeping alongside require.Same because
	// it also catches a rebind to a different-but-equally-dead registry, which
	// pointer identity would flag only by accident.
	require.NotNil(t, craftGetBlockMessage(t, chainParser).GetRPCMessage().GetParams(),
		"MAG-2538: the live parser's registry must still resolve descriptors after recovery")

	// Smoke check, NOT a regression assertion. SendNodeMsg builds its descriptor
	// source from the ChainProxy's own connector and never reads GrpcMessage.Registry,
	// so it cannot observe this ticket's defect — verified: it passes with the clone
	// removed from validateProvider. What it does prove is the adjacent property that
	// the serving connector itself outlived recovery's teardown.
	reply, _, _, _, _, err := servingRouter.SendNodeMsg(ctx, nil, craftGetBlockMessage(t, chainParser), nil)
	require.NoError(t, err, "the serving connector must survive background recovery")
	require.NotNil(t, reply)

	// Vacuity guard, deliberately last so it cannot disturb the assertions above.
	// The same endpoint DOES rebind the parser when handed it directly, which is
	// what makes the require.Same above a real constraint rather than a tautology.
	// (An earlier attempt at this test pointed at an unreachable address; the dial
	// failed before setupForProvider ran, so it passed against the broken code too.)
	_, err = chainlib.GetChainRouter(ctx, 1, endpoint, chainParser)
	require.NoError(t, err)
	require.NotSame(t, servingRegistry, boundRegistry(t, craftGetBlockMessage(t, chainParser)),
		"control: an unprotected GetChainRouter must rebind the live parser")
}
