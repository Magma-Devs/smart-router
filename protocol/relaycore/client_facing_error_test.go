package relaycore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// upstream503Err is what the direct JSON-RPC sender hands the processor for a node's 503:
// the typed status error wrapped with its classification (classifyAndWrap).
func upstream503Err(body string) error {
	return common.NewLavaErrorWrapping(common.LavaErrorNodeServiceUnavailable, &lavasession.HTTPStatusError{
		StatusCode: http.StatusServiceUnavailable,
		Status:     "503 Service Unavailable",
		Body:       []byte(body),
	})
}

// refusedErr is the same for a dial that was refused: net/http's *url.Error, wrapped.
func refusedErr() error {
	dial := &url.Error{Op: "Post", URL: "http://127.0.0.1:18545", Err: &net.OpError{
		Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED),
	}}
	return common.NewLavaErrorWrapping(common.LavaErrorConnectionRefused, fmt.Errorf("http request failed: %w", dial))
}

func TestClientFacingFromProtocolError_UpstreamAnswered(t *testing.T) {
	cf := clientFacingFromProtocolError(upstream503Err("down"), common.LavaErrorNodeServiceUnavailable)
	require.NotNil(t, cf)
	require.Equal(t, http.StatusServiceUnavailable, cf.UpstreamStatus)
	require.Equal(t, "503 Service Unavailable", cf.UpstreamStatusText)
	require.Equal(t, []byte("down"), cf.UpstreamBody)
	require.Empty(t, cf.Cause, "the upstream answered, so there is no transport cause")
	require.Equal(t, common.LavaErrorNodeServiceUnavailable.Code, cf.RouterErrorCode)
	require.Equal(t, common.LavaErrorNodeServiceUnavailable.Name, cf.RouterErrorName)
}

// Every code that means "the upstream never answered" yields the leaf cause and no status.
func TestClientFacingFromProtocolError_TransportFailures(t *testing.T) {
	leaf := errors.New("dial tcp 10.0.0.1:443: connect: connection refused")
	for _, lavaErr := range []*common.LavaError{
		common.LavaErrorConnectionTimeout,
		common.LavaErrorConnectionRefused,
		common.LavaErrorDNSFailure,
		common.LavaErrorTLSMismatch,
		common.LavaErrorConnectionReset,
		common.LavaErrorConnectionClosed,
		common.LavaErrorContextDeadline,
	} {
		t.Run(lavaErr.Name, func(t *testing.T) {
			cf := clientFacingFromProtocolError(common.NewLavaErrorWrapping(lavaErr, leaf), lavaErr)
			require.NotNil(t, cf)
			require.Zero(t, cf.UpstreamStatus)
			require.Equal(t, leaf.Error(), cf.Cause, "the retained cause, not the registry description")
			require.Equal(t, lavaErr.Code, cf.RouterErrorCode)
		})
	}
}

// Anything else keeps today's reply: no client-facing answer at all.
func TestClientFacingFromProtocolError_OtherErrorsKeepTodaysReply(t *testing.T) {
	require.Nil(t, clientFacingFromProtocolError(nil, common.LavaErrorConnectionRefused))
	// A cancel says when the router stopped waiting, not that the upstream failed.
	require.Nil(t, clientFacingFromProtocolError(
		common.NewLavaErrorWrapping(common.LavaErrorContextCanceled, context.Canceled), common.LavaErrorContextCanceled))
	require.Nil(t, clientFacingFromProtocolError(errors.New("no providers"), common.LavaErrorNoProviders))
	require.Nil(t, clientFacingFromProtocolError(errors.New("unclassified"), nil))
	// A status below 400 is not an upstream error answer.
	require.Nil(t, clientFacingFromProtocolError(&lavasession.HTTPStatusError{StatusCode: http.StatusFound, Status: "302 Found"}, nil))
}

func TestLeafCause(t *testing.T) {
	inner := errors.New("connect: connection refused")
	// The registry wrapper keeps the original: that is what the client should read.
	require.Equal(t, inner.Error(), leafCause(common.NewLavaErrorWrapping(common.LavaErrorConnectionRefused, inner)))
	// A wrapper above the registry wrapper does not hide it.
	require.Equal(t, inner.Error(), leafCause(fmt.Errorf("relay: %w", common.NewLavaErrorWrapping(common.LavaErrorConnectionRefused, inner))))
	// No registry wrapper: the innermost error of a %w chain.
	require.Equal(t, inner.Error(), leafCause(fmt.Errorf("a: %w", fmt.Errorf("b: %w", inner))))
	require.Equal(t, "plain", leafCause(errors.New("plain")))
}

// storedProtocolResults returns the RelayResults the processor keeps for its protocol errors:
// the shared state the failure stamp must never write to.
func storedProtocolResults(t *testing.T, rp *RelayProcessor) []common.RelayResult {
	t.Helper()
	inst, ok := rp.ResultsManager.(*ResultsManagerInst)
	require.True(t, ok)
	inst.lock.RLock()
	defer inst.lock.RUnlock()
	results := make([]common.RelayResult, 0, len(inst.protocolResponseErrors.RelayErrors))
	for _, relayErr := range inst.protocolResponseErrors.RelayErrors {
		require.NotNil(t, relayErr.Response)
		results = append(results, relayErr.Response.RelayResult)
	}
	return results
}

func waitForBoth(t *testing.T, rp *RelayProcessor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = rp.WaitForResults(ctx)
}

// Every provider answered 503: the client gets 503 and the upstream's answer, on a copy.
func TestProcessingResult_Upstream503IsForwarded(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()

	go SendProtocolError(rp, "lava@a", time.Millisecond, upstream503Err("node a down"))
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, upstream503Err("node b down"))
	waitForBoth(t, rp)

	result, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, result.StatusCode)
	cf := result.GetClientFacing()
	require.NotNil(t, cf)
	require.Equal(t, http.StatusServiceUnavailable, cf.UpstreamStatus)
	require.Contains(t, []string{"node a down", "node b down"}, string(cf.UpstreamBody))
	require.Equal(t, common.LavaErrorNodeServiceUnavailable.Code, cf.RouterErrorCode, "the classification is unchanged")

	for _, stored := range storedProtocolResults(t, rp) {
		require.Zero(t, stored.StatusCode, "the stored result must keep its own status")
		require.Nil(t, stored.ClientFacing, "the stored result must not be stamped")
	}
}

// No provider answered: 502 and the cause, on a copy.
func TestProcessingResult_TransportFailureIs502(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()

	go SendProtocolError(rp, "lava@a", time.Millisecond, refusedErr())
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, refusedErr())
	waitForBoth(t, rp)

	result, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusBadGateway, result.StatusCode)
	cf := result.GetClientFacing()
	require.NotNil(t, cf)
	require.Zero(t, cf.UpstreamStatus)
	require.Contains(t, cf.Cause, "connection refused")
	require.Equal(t, common.LavaErrorConnectionRefused.Code, cf.RouterErrorCode)

	for _, stored := range storedProtocolResults(t, rp) {
		require.Zero(t, stored.StatusCode)
		require.Nil(t, stored.ClientFacing)
	}
}

// The stamp is per call: sendRelayWithRetries calls ProcessingResult once per iteration on one
// processor, so the stored results must come out of every call as they went in.
func TestProcessingResult_ClientFacingDoesNotStick(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()

	go SendProtocolError(rp, "lava@a", time.Millisecond, refusedErr())
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, refusedErr())
	waitForBoth(t, rp)
	first, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusBadGateway, first.StatusCode)

	for _, stored := range storedProtocolResults(t, rp) {
		require.Zero(t, stored.StatusCode, "a second ProcessingResult reads these again")
		require.Nil(t, stored.ClientFacing)
	}
	second, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusBadGateway, second.StatusCode, "same inputs, same answer")
	require.NotSame(t, first, second, "each call returns its own copy")
}

// Every attempt was refused for rate: the 503 stamp still wins, and nothing is forwarded
// because a typed 429 is not an upstream status error on this path.
func TestProcessingResult_AllRateLimitedStill503WithoutClientFacing(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()

	go SendProtocolError(rp, "lava@a", time.Millisecond, common.RateLimited(errors.New("HTTP 429"), 0))
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, common.RateLimited(errors.New("HTTP 429"), 0))
	waitForBoth(t, rp)

	result, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, result.StatusCode)
	require.Nil(t, result.GetClientFacing())
}

// A node error beats a protocol error, as before, and is never stamped: the node's reply is
// already what the client gets.
func TestProcessingResult_NodeErrorIsPreferredAndNotStamped(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()

	go SendNodeError(rp, "lava@a", time.Millisecond)
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, upstream503Err("node b down"))
	waitForBoth(t, rp)

	result, _ := rp.ProcessingResult()
	require.NotNil(t, result)
	require.Nil(t, result.GetClientFacing())
	require.NotEqual(t, http.StatusServiceUnavailable, result.StatusCode)
}

func TestGetClientFacing_NilSafe(t *testing.T) {
	var rr *common.RelayResult
	require.Nil(t, rr.GetClientFacing())
}

func vendor503Err(statusLine string) error {
	return common.NewLavaErrorWrapping(common.LavaErrorNodeServiceUnavailable, &lavasession.HTTPStatusError{
		StatusCode: http.StatusServiceUnavailable,
		Status:     statusLine,
	})
}

// Two vendors that word their 503 differently are still the same error to the picker, so the
// 503 majority wins over a lone refused connection every time. Before the error text was made
// canonical, the picker chose between the three at random and about one request in nine got
// the 502 "no answer from upstream" although two nodes had answered (MAG-4218).
func TestGetBestErrorMessageForUser_MixedVendor503sStayOneMajority(t *testing.T) {
	errs := []RelayError{
		{Err: vendor503Err("503 Service Temporarily Unavailable"), ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@nginx"}, LavaError: common.LavaErrorNodeServiceUnavailable},
		{Err: vendor503Err("503 Service Unavailable"), ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@go"}, LavaError: common.LavaErrorNodeServiceUnavailable},
		{Err: refusedErr(), ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@refused"}, LavaError: common.LavaErrorConnectionRefused},
	}
	// The map the picker groups by is iterated in random order, so one run proves nothing.
	for i := 0; i < 200; i++ {
		picked := (&RelayErrors{RelayErrors: errs}).GetBestErrorMessageForUser()
		require.NotEqual(t, "lava@refused", picked.ProviderInfo.ProviderAddress, "iteration %d", i)
	}
}

// End to end through the processor: the client gets 503 and one of the vendors' own status
// lines, never the canonical text the router decided on.
func TestProcessingResult_MixedVendor503sForwardTheVendorsLine(t *testing.T) {
	for i := 0; i < 20; i++ {
		rp, _, closer := newTwoProviderProcessor(t)
		go SendProtocolError(rp, "lava@a", time.Millisecond, vendor503Err("503 Service Temporarily Unavailable"))
		go SendProtocolError(rp, "lava@b", 2*time.Millisecond, vendor503Err("503 Service Unavailable"))
		waitForBoth(t, rp)

		result, err := rp.ProcessingResult()
		closer()
		require.Error(t, err)
		require.Equal(t, http.StatusServiceUnavailable, result.StatusCode)
		cf := result.GetClientFacing()
		require.NotNil(t, cf)
		require.Contains(t, []string{"503 Service Temporarily Unavailable", "503 Service Unavailable"}, cf.UpstreamStatusText)
	}
}

// One upstream answered 503 and the other refused the connection: a one-to-one tie the majority
// pick breaks at random. The client gets the upstream's 503 whichever error arrives first, never
// the 502 "no answer from upstream" (MAG-4218).
func TestProcessingResult_UpstreamAnswerBeatsTransportFailure(t *testing.T) {
	for _, answerFirst := range []bool{true, false} {
		answerDelay, refusedDelay := time.Millisecond, 2*time.Millisecond
		if !answerFirst {
			answerDelay, refusedDelay = refusedDelay, answerDelay
		}
		for i := 0; i < 50; i++ {
			rp, _, closer := newTwoProviderProcessor(t)
			go SendProtocolError(rp, "lava@a", answerDelay, upstream503Err("node a down"))
			go SendProtocolError(rp, "lava@b", refusedDelay, refusedErr())
			waitForBoth(t, rp)

			result, err := rp.ProcessingResult()
			closer()
			require.Error(t, err)
			require.Equal(t, http.StatusServiceUnavailable, result.StatusCode, "answer first: %v", answerFirst)
			cf := result.GetClientFacing()
			require.NotNil(t, cf)
			require.Equal(t, "node a down", string(cf.UpstreamBody))
		}
	}
}

// A transport failure is the reply only when every attempt failed in transport: next to any
// other error, that error's reply stands, whichever arrives first.
func TestReplyProtocolError_TransportFailureIsTheLastResort(t *testing.T) {
	answered := RelayError{Err: upstream503Err("node a down"), LavaError: common.LavaErrorNodeServiceUnavailable}
	refused := RelayError{Err: refusedErr(), LavaError: common.LavaErrorConnectionRefused}
	other := RelayError{Err: errors.New("no providers"), LavaError: common.LavaErrorNoProviders}

	for _, picked := range []RelayError{answered, refused, other} {
		require.Equal(t, answered.Err, replyProtocolError(picked, []RelayError{refused, other, answered}).Err)
	}
	require.Equal(t, other.Err, replyProtocolError(refused, []RelayError{refused, other}).Err)
	require.Equal(t, refused.Err, replyProtocolError(refused, []RelayError{refused, refused}).Err)
}
