package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// An attempt that finishes after the request has ended must not park its goroutine forever.
//
// Attempts used to die staggered at their own window, while a reader was re-armed between batches,
// so a full response buffer always drained. Since the attempt lifetime became the request budget
// they all finish together, AFTER the last reader has gone — and every attempt past the buffer's
// capacity blocked in SetResponse for the life of the process, pinning the RelayProcessor with it.
// One leaked goroutine per request.
func newProcessorForLeakTest(t *testing.T, ctx context.Context, selection Selection) *RelayProcessor {
	t.Helper()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeServer != nil {
			closeServer()
		}
	})
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "1.2.3.4")
	usedProviders := lavasession.NewUsedProviders(nil)
	// CrossValidation refuses to construct without params, and it is the one selection that still
	// reads this channel after the request has ended — so it has to be expressible here.
	var crossValidationParams *common.CrossValidationParams
	if selection == CrossValidation {
		params := common.DefaultCrossValidationParams
		crossValidationParams = &params
	}
	return NewRelayProcessor(ctx, crossValidationParams, RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance,
		newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, selection))
}

// The buffer is full and the request is over. SetResponse has to return rather than block.
// Against the pre-fix unconditional send this test hangs and is killed by the -timeout.
func TestSetResponse_DoesNotParkOnceTheRequestIsOver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rp := newProcessorForLeakTest(t, ctx, Stateless)

	// Fill the buffer exactly, with nobody reading.
	for i := 0; i < cap(rp.responses); i++ {
		rp.SetResponse(&RelayResponse{})
	}
	require.Equal(t, cap(rp.responses), len(rp.responses), "buffer should be full before the real assertion")

	// The request ends. Every remaining in-flight attempt now reports in.
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 8; i++ { // comfortably more than the buffer can hold
			rp.SetResponse(&RelayResponse{})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SetResponse blocked after the request ended — this is the leak: one goroutine, and the RelayProcessor it pins, per request")
	}
}

// While the request is still alive a full buffer is transient and the response is still wanted, so
// SetResponse must block rather than drop it. This is the other half of the contract: the fix must
// not turn a temporarily-full buffer into lost results.
func TestSetResponse_StillBlocksWhileTheRequestIsAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rp := newProcessorForLeakTest(t, ctx, Stateless)

	for i := 0; i < cap(rp.responses); i++ {
		rp.SetResponse(&RelayResponse{})
	}

	sent := make(chan struct{})
	go func() {
		rp.SetResponse(&RelayResponse{}) // must wait for room, not vanish
		close(sent)
	}()

	select {
	case <-sent:
		t.Fatal("SetResponse dropped a response while the request was still reading")
	case <-time.After(200 * time.Millisecond):
	}

	// Make room; the pending send must then complete rather than have been discarded.
	<-rp.responses
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("SetResponse did not deliver once the buffer drained")
	}
}

// The third state, and the one that actually happens on every cross-validation request: the buffer
// has ROOM and the request is over. A straggler is detached from the batch cancel precisely so it
// outlives the reply, and rp.ctx — ProcessRelaySend's — is cancelled the moment the quorum
// early-exits, which is before watchCrossValidationStragglers has even started. So the give-up arm
// is ready for every late response, not for the rare one.
//
// With the send and the give-up in one select, both arms are ready and Go picks at random: about
// half of all straggler responses were discarded. A discarded one never reaches handleResponse
// either, so resolveFromRecorded cannot recover it and the watcher reports not-received for a
// provider that answered — and its content is never compared against the consensus, losing the late
// dissent that only this path can see.
//
// Looped because the bug is a coin flip: one iteration passed half the time.
func TestSetResponse_DeliversToTheStragglerWatcherAfterTheRequestEnded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rp := newProcessorForLeakTest(t, ctx, CrossValidation)

	// ProcessRelaySend returns on the quorum early-exit and its deferred cancel runs. The watcher
	// starts after this point, so every straggler below pushes into a cancelled ctx.
	cancel()

	const stragglers = 200
	for i := 0; i < stragglers; i++ {
		rp.SetResponse(&RelayResponse{})
		require.Lenf(t, rp.responses, 1,
			"straggler %d was dropped with room in the buffer (cap %d) — the watcher is about to read it, "+
				"and will now report the provider not-received instead", i, cap(rp.responses))
		<-rp.responses // stand in for the watcher, so the next send still has room
	}
}
