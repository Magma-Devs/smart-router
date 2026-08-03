package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-2550 — the recovery-probe pair: recordRelayProbeEvidence (relay path, eligibility) and
// replayFailingRelay (probe loop, judge), plus the probe-cycle integration of the two-step
// re-enable (gate → replay → confirm/fail).

// ---------------------------------------------------------------------------
// Probe-cycle integration: gate → replay → confirm / fail
// ---------------------------------------------------------------------------

var testEvidence = []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}`)

// relayProbeRecorder is a fake relayProbeFunc that records invocations and returns a scripted
// verdict per call.
type relayProbeRecorder struct {
	verdicts []bool // consumed in order; the last value repeats
	calls    int
	method   string
	payload  []byte
}

func (r *relayProbeRecorder) probe(ep *lavasession.EndpointWithDirectConnection, method string, payload []byte) bool {
	r.calls++
	r.method = method
	r.payload = payload
	idx := r.calls - 1
	if idx >= len(r.verdicts) {
		idx = len(r.verdicts) - 1
	}
	return r.verdicts[idx]
}

// TestProbeCycle_RelayEvidenceGatesReEnableUntilReplayPasses: an endpoint whose disable episode
// recorded a failing relay is NOT re-enabled by poll hysteresis alone — the cycle replays the
// recorded request at the threshold, and only a passing replay re-enables (counting as the
// cycle's re-enable and restoring routing).
func TestProbeCycle_RelayEvidenceGatesReEnableUntilReplayPasses(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	probe := &relayProbeRecorder{verdicts: []bool{true}}
	var recovered []string
	cycle := func(i int) int {
		pollTime := base.Add(time.Duration(i) * time.Second)
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		_, reEnabled, _ := runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{},
			now, probeCfg(), nil, func(p string) { recovered = append(recovered, p) }, probe.probe)
		return reEnabled
	}

	K := int(probeCfg().ReEnableHysteresis)
	for i := 1; i < K; i++ {
		require.Equal(t, 0, cycle(i))
		require.Zero(t, probe.calls, "no replay before the poll hysteresis is earned")
		require.False(t, dc.Endpoint.Enabled)
	}
	// The K-th cycle replays the recorded request and, on success, re-enables in the same cycle.
	require.Equal(t, 1, cycle(K), "a passing replay counts as the cycle's re-enable")
	require.Equal(t, 1, probe.calls, "exactly one replay at the threshold")
	require.Equal(t, "eth_call", probe.method)
	require.Equal(t, testEvidence, probe.payload, "the replay receives the exact recorded request")
	require.True(t, dc.Endpoint.Enabled)
	require.Equal(t, []string{"provider1"}, recovered, "a replay-confirmed recovery restores routing (F2)")
}

// TestProbeCycle_FailedReplayKeepsDisabledAndPacesRetries: a failing replay keeps the endpoint
// disabled, resets the streak, and escalates — the next replay happens only after the escalated
// K<<1 streak is re-earned, NOT on every cycle.
func TestProbeCycle_FailedReplayKeepsDisabledAndPacesRetries(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	probe := &relayProbeRecorder{verdicts: []bool{false, true}}
	cycle := func(i int) int {
		pollTime := base.Add(time.Duration(i) * time.Second)
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		_, reEnabled, _ := runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{},
			now, probeCfg(), nil, nil, probe.probe)
		return reEnabled
	}

	K := int(probeCfg().ReEnableHysteresis)
	for i := 1; i <= K; i++ {
		require.Equal(t, 0, cycle(i), "a failing replay must not re-enable")
	}
	require.Equal(t, 1, probe.calls, "one replay at the threshold, which failed")
	require.False(t, dc.Endpoint.Enabled)

	// The next K cycles re-earn only the BASE streak — below the escalated K<<1 → no replay yet.
	for i := K + 1; i <= 2*K; i++ {
		require.Equal(t, 0, cycle(i))
	}
	require.Equal(t, 1, probe.calls, "no replay until the escalated K<<1 streak is re-earned (pacing)")

	// Completing the escalated streak triggers the second replay, which passes → re-enabled.
	var last int
	for i := 2*K + 1; i <= 3*K; i++ {
		last = cycle(i)
	}
	require.Equal(t, 2, probe.calls, "second replay exactly when the escalated streak completes")
	require.Equal(t, 1, last, "the passing replay's cycle reports the re-enable")
	require.True(t, dc.Endpoint.Enabled)
}

// TestProbeCycle_NilRelayProbeFallsBackToPollOnly: without replay wiring (nil relayProbe — tests,
// degraded setups) recorded evidence must not park the endpoint forever; the cycle falls back to
// poll-only evidence like the legacy path.
func TestProbeCycle_NilRelayProbeFallsBackToPollOnly(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	K := int(probeCfg().ReEnableHysteresis)
	for i := 1; i <= K; i++ {
		pollTime := base.Add(time.Duration(i) * time.Second)
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{}, now, probeCfg(), nil, nil, nil)
	}
	require.True(t, dc.Endpoint.Enabled, "nil relayProbe falls back to poll-only re-enable instead of parking the endpoint")
}

// ---------------------------------------------------------------------------
// replayFailingRelay — the judge
// ---------------------------------------------------------------------------

// fakeDirectConn is a scriptable DirectRPCConnection for the replay judge.
type fakeDirectConn struct {
	resp       *lavasession.DirectRPCResponse
	err        error
	gotPayload []byte
}

func (f *fakeDirectConn) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	f.gotPayload = data
	return f.resp, f.err
}
func (f *fakeDirectConn) GetProtocol() lavasession.DirectRPCProtocol { return "http" }
func (f *fakeDirectConn) Close() error                               { return nil }
func (f *fakeDirectConn) GetURL() string                             { return "http://fake:8545" }
func (f *fakeDirectConn) GetNodeUrl() *common.NodeUrl                { return nil }

func replayServer() *RPCSmartRouterServer {
	return &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
}

func replayWith(t *testing.T, conn *fakeDirectConn) bool {
	t.Helper()
	rpcss := replayServer()
	epc := &lavasession.EndpointWithDirectConnection{
		Endpoint:         &lavasession.Endpoint{NetworkAddress: "http://fake:8545"},
		DirectConnection: conn,
		ProviderAddress:  "provider1",
	}
	return rpcss.replayFailingRelay(epc, "eth_call", testEvidence)
}

// TestReplayFailingRelay_JudgesByHealthClassification pins the judge's contract: "pass" means the
// request no longer produces a HEALTH-AFFECTING failure — the same rule the relay path uses to
// mark endpoints unhealthy — not "the request returned a happy result".
func TestReplayFailingRelay_JudgesByHealthClassification(t *testing.T) {
	cases := []struct {
		name string
		conn *fakeDirectConn
		want bool
	}{
		{"clean 200 result", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}}, true},
		{"5xx still failing", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 503, Status: "503"}}, false},
		{"429 proves nothing", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 429, Status: "429"}}, false},
		{"501 is a capability answer, not a fault", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 501, Status: "501"}}, true},
		{"transport error still failing", &fakeDirectConn{err: context.DeadlineExceeded}, false},
		{"node internal error rides HTTP 200", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`)}}, false},
		{"unsupported method in body is not a fault", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`)}}, true},
		{"batch reply (JSON array) passes on transport success", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`)}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, replayWith(t, tc.conn))
			require.Equal(t, testEvidence, tc.conn.gotPayload, "the judge sends the exact recorded bytes")
		})
	}
}

func TestReplayFailingRelay_NoConnectionFails(t *testing.T) {
	rpcss := replayServer()
	require.False(t, rpcss.replayFailingRelay(nil, "eth_call", testEvidence))
	require.False(t, rpcss.replayFailingRelay(&lavasession.EndpointWithDirectConnection{}, "eth_call", testEvidence))
}

// ---------------------------------------------------------------------------
// recordRelayProbeEvidence — eligibility
// ---------------------------------------------------------------------------

func mockMessage(t *testing.T, api *spectypes.Api, parseDirective *spectypes.ParseDirective) *chainlib.MockChainMessage {
	t.Helper()
	ctrl := gomock.NewController(t)
	msg := chainlib.NewMockChainMessage(ctrl)
	msg.EXPECT().GetApi().Return(api).AnyTimes()
	msg.EXPECT().GetParseDirective().Return(parseDirective).AnyTimes()
	return msg
}

// TestRecordRelayProbeEvidence_Eligibility pins the recording rules: read-only JSON-RPC methods
// are recorded; writes (stateful), subscriptions, hanging APIs, and non-JSON-RPC interfaces are
// not — those episodes fall back to the poll-only path with its trial budget.
func TestRecordRelayProbeEvidence_Eligibility(t *testing.T) {
	readOnly := &spectypes.Api{Name: "eth_call"}
	write := &spectypes.Api{Name: "eth_sendRawTransaction", Category: spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS}}
	hanging := &spectypes.Api{Name: "eth_newFilter", Category: spectypes.SpecCategory{HangingApi: true}}
	subscribe := &spectypes.Api{Name: "eth_subscribe"}
	subscribeDirective := &spectypes.ParseDirective{ApiName: "eth_subscribe", FunctionTag: spectypes.FUNCTION_TAG_SUBSCRIBE}

	cases := []struct {
		name         string
		apiInterface string
		msg          chainlib.ChainMessage
		wantRecorded bool
	}{
		{"read-only jsonrpc method is recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, readOnly, nil), true},
		{"write method never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, write, nil), false},
		{"hanging api never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, hanging, nil), false},
		{"subscription never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, subscribe, subscribeDirective), false},
		{"non-jsonrpc interface never recorded", spectypes.APIInterfaceRest, mockMessage(t, readOnly, nil), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rpcss := &RPCSmartRouterServer{
				listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: tc.apiInterface},
			}
			endpoint := &lavasession.Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
			rpcss.recordRelayProbeEvidence(endpoint, tc.msg, testEvidence)
			recorded := endpoint.HealthSnapshot().RelayProbeMethod != ""
			require.Equal(t, tc.wantRecorded, recorded)
		})
	}
}
