package rpcsmartrouter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// MAG-4617: what a finished client relay tells readiness. Only an upstream's real answer proves
// the chain healthy; a node error, a timeout or no answer brings the probe forward; a cache hit
// and a client that hung up say nothing about the upstreams.
func TestRelayHealthSignalOf(t *testing.T) {
	reply := &pairingtypes.RelayReply{Data: []byte(`{"result":"0x10"}`)}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name   string
		ctx    context.Context
		result *common.RelayResult
		err    error
		want   relayHealthSignal
	}{
		{name: "an upstream answered", result: &common.RelayResult{Reply: reply, StatusCode: 200}, want: relayHealthServed},
		{name: "a gRPC upstream answered", result: &common.RelayResult{Reply: reply}, want: relayHealthServed},
		{name: "a node error", result: &common.RelayResult{Reply: reply, IsNodeError: true}, want: relayHealthFailed},
		{name: "a rate-limited node error", result: &common.RelayResult{Reply: reply, IsNodeError: true, IsRateLimited: true}, want: relayHealthFailed},
		{name: "an upstream 503", result: &common.RelayResult{Reply: reply, StatusCode: 503}, want: relayHealthFailed},
		{name: "no upstream answered", err: errors.New("failed relay, insufficient results"), want: relayHealthFailed},
		{name: "neither an answer nor an error", want: relayHealthFailed},
		{name: "a cache hit", result: &common.RelayResult{Reply: reply, CacheLookup: common.CacheLookupReport{ServedTier: common.CacheTierPrimary}}, want: relayHealthNone},
		{name: "a cached node error", result: &common.RelayResult{Reply: reply, IsNodeError: true, CacheLookup: common.CacheLookupReport{ServedTier: common.CacheTierSecondary}}, want: relayHealthNone},
		{name: "the client hung up", ctx: cancelled, err: context.Canceled, want: relayHealthNone},
		{name: "a cancellation the client did not make", err: context.Canceled, want: relayHealthFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			require.Equal(t, tc.want, relayHealthSignalOf(ctx, tc.result, tc.err))
		})
	}
}

// The wiring: SendParsedRelay hands every finished client relay to the chain's readiness monitor.
func TestSendParsedRelay_FeedsReadiness(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reply      string
		probesSoon bool
	}{
		{name: "an error answer brings the probe forward", reply: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`, probesSoon: true},
		{name: "a result leaves it an interval away", reply: `{"jsonrpc":"2.0","id":1,"result":"0x10"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fleet := newHealthRelayFleetAnswering(t, nil, tc.reply)
			var probes atomic.Int32
			monitor := metrics.NewRelaysMonitor(time.Hour, 20*time.Millisecond, "ETH1", "jsonrpc")
			monitor.SetRelaySender(func() (bool, error) { probes.Add(1); return true, nil })
			monitor.Start(t.Context())
			require.Eventually(t, func() bool { return probes.Load() == 1 }, time.Second, time.Millisecond)
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
