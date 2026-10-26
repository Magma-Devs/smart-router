package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// MAG-4641: what a relay's attempts say about whether this replica reaches its upstreams. An
// answer of any kind counts, on positive evidence only; anything else, an uncatalogued failure
// included, is no answer.
func TestUpstreamAnswerFrom(t *testing.T) {
	reply := &pairingtypes.RelayReply{Data: []byte(`{"result":"0x10"}`)}
	failed := func(err error, result common.RelayResult) relaycore.RelayError {
		return relaycore.RelayError{Err: err, Response: &relaycore.RelayResponse{RelayResult: result, Err: err}}
	}

	for _, tc := range []struct {
		name           string
		successes      []common.RelayResult
		nodeErrors     []relaycore.RelayError
		protocolErrors []relaycore.RelayError
		want           upstreamAnswer
	}{
		{name: "a result", successes: []common.RelayResult{{Reply: reply, StatusCode: 200}}, want: upstreamAnswerResult},
		{name: "a gRPC result", successes: []common.RelayResult{{Reply: reply}}, want: upstreamAnswerResult},
		{name: "a result after a timeout", successes: []common.RelayResult{{Reply: reply, StatusCode: 200}},
			protocolErrors: []relaycore.RelayError{failed(context.DeadlineExceeded, common.RelayResult{})}, want: upstreamAnswerResult},
		{name: "a node error", nodeErrors: []relaycore.RelayError{{Err: errors.New("execution reverted")}}, want: upstreamAnswerError},
		{name: "a JSON-RPC error status", protocolErrors: []relaycore.RelayError{
			failed(fmt.Errorf("relay failed: %w", &lavasession.HTTPStatusError{StatusCode: 503}), common.RelayResult{})}, want: upstreamAnswerError},
		{name: "a REST error status", protocolErrors: []relaycore.RelayError{
			failed(errors.New("HTTP 503"), common.RelayResult{IsNodeError: true})}, want: upstreamAnswerError},
		{name: "a typed rate limit", protocolErrors: []relaycore.RelayError{
			failed(fmt.Errorf("HTTP 429: %w", common.StatusCodeError429), common.RelayResult{})}, want: upstreamAnswerError},
		{name: "a classified rate limit", protocolErrors: []relaycore.RelayError{
			{Err: errors.New("slow down"), LavaError: common.LavaErrorNodeRateLimited}}, want: upstreamAnswerError},
		{name: "a refused connection", protocolErrors: []relaycore.RelayError{
			failed(errors.New("dial tcp 10.0.0.1:443: connect: connection refused"), common.RelayResult{})}, want: upstreamAnswerNone},
		{name: "a timeout", protocolErrors: []relaycore.RelayError{
			failed(context.DeadlineExceeded, common.RelayResult{})}, want: upstreamAnswerNone},
		{name: "an attempt the router stopped", protocolErrors: []relaycore.RelayError{
			failed(context.Canceled, common.RelayResult{})}, want: upstreamAnswerNone},
		{name: "a failure nobody has catalogued", protocolErrors: []relaycore.RelayError{
			{Err: errors.New("something new")}}, want: upstreamAnswerNone},
		{name: "nothing sent", want: upstreamAnswerNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, upstreamAnswerFrom(tc.successes, tc.nodeErrors, tc.protocolErrors))
		})
	}
}

// The carve-outs that come before the attempts are read: a client that hung up and a cache hit say
// nothing about the upstreams. A relay that never got a processor sent nothing.
func TestRelayHealthSignalOf_BeforeTheAttempts(t *testing.T) {
	reply := &pairingtypes.RelayReply{Data: []byte(`{"result":"0x10"}`)}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	require.Equal(t, relayHealthNone, relayHealthSignalOf(cancelled, nil, nil, context.Canceled), "the client hung up")
	require.Equal(t, relayHealthNone, relayHealthSignalOf(context.Background(), nil,
		&common.RelayResult{Reply: reply, CacheLookup: common.CacheLookupReport{ServedTier: common.CacheTierPrimary}}, nil), "a cache hit")
	require.Equal(t, relayHealthUnanswered, relayHealthSignalOf(context.Background(), nil, nil, context.Canceled),
		"a cancellation the client did not make")
	require.Equal(t, relayHealthUnanswered, relayHealthSignalOf(context.Background(), nil, nil, errors.New("no providers")),
		"nothing sent")
}

// The wiring: SendParsedRelay hands every finished client relay to the chain's readiness monitor.
// Only a relay no upstream answered brings the probe forward.
func TestSendParsedRelay_FeedsReadiness(t *testing.T) {
	answering := func(status int, body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}
	}
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		probesSoon bool
	}{
		{name: "a result leaves the probe an interval away", handler: answering(http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`)},
		{name: "an error answer leaves it there too", handler: answering(http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`)},
		{name: "an error status leaves it there too", handler: answering(http.StatusServiceUnavailable, `<html>origin down</html>`)},
		{name: "no answer brings the probe forward", handler: dropConnection, probesSoon: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fleet := newHealthRelayFleetServing(t, nil, tc.handler)
			var probes atomic.Int32
			monitor := metrics.NewRelaysMonitor(time.Hour, 20*time.Millisecond, "ETH1", "jsonrpc")
			monitor.SetRelaySender(func() metrics.ProbeVerdict { probes.Add(1); return metrics.ProbeHealthy })
			monitor.Start(t.Context())
			require.Eventually(t, func() bool { return probes.Load() == 1 }, time.Second, time.Millisecond)
			// The boot probe's verdict is recorded just after its sender returns; let it land, or
			// it would replace the cadence this relay sets.
			time.Sleep(50 * time.Millisecond)
			fleet.server.relaysMonitor = monitor

			_, _ = fleet.server.SendParsedRelay(t.Context(), nil, fleet.craftedMessage(t))
			require.Positive(t, fleet.served.Load(), "premise: the relay reached the upstream")

			if tc.probesSoon {
				require.Eventually(t, func() bool { return probes.Load() == 2 }, time.Second, time.Millisecond)
				return
			}
			require.Never(t, func() bool { return probes.Load() > 1 }, 200*time.Millisecond, time.Millisecond)
		})
	}
}
