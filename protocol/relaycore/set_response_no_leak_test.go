package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
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
	return NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance,
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
