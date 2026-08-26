package rpcsmartrouter

import (
	"context"
	"sync/atomic"
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
// replayFailingRelay (probe loop, three-way judge), plus the probe-cycle integration of the
// two-step re-enable (gate → replay → confirm / still-failing / inconclusive) through the
// relayProbeRunner (replays off the cycle, single-flight per endpoint).

// ---------------------------------------------------------------------------
// Probe-cycle integration: gate → replay → confirm / fail
// ---------------------------------------------------------------------------

var testEvidence = []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}`)

const testEvidenceTimeout = 15 * time.Second

// relayProbeRecorder is a fake relayProbeFunc that records invocations and returns a scripted
// verdict per call.
type relayProbeRecorder struct {
	verdicts []relayProbeVerdict // consumed in order; the last value repeats
	calls    int
	method   string
	payload  []byte
	timeout  time.Duration
}

func (r *relayProbeRecorder) probe(ep *lavasession.EndpointWithDirectConnection, method string, payload []byte, relayTimeout time.Duration) relayProbeVerdict {
	r.calls++
	r.method = method
	r.payload = payload
	r.timeout = relayTimeout
	idx := r.calls - 1
	if idx >= len(r.verdicts) {
		idx = len(r.verdicts) - 1
	}
	return r.verdicts[idx]
}

// syncRunner wraps a recorder in a relayProbeRunner that executes inline (async=false), so the
// cycle tests stay deterministic while exercising the same launch path production uses.
func syncRunner(probe *relayProbeRecorder) *relayProbeRunner {
	return &relayProbeRunner{probe: probe.probe}
}

// TestProbeCycle_RelayEvidenceGatesReEnableUntilReplayPasses: an endpoint whose disable episode
// recorded a failing relay is NOT re-enabled by poll hysteresis alone — the cycle replays the
// recorded request (with its recorded timeout) at the threshold, and only a passing replay
// re-enables and restores routing. Replay-granted re-enables complete on the replayer, so they are
// NOT part of the cycle's synchronous reEnabled count.
func TestProbeCycle_RelayEvidenceGatesReEnableUntilReplayPasses(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence, testEvidenceTimeout)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	probe := &relayProbeRecorder{verdicts: []relayProbeVerdict{relayProbeRecovered}}
	runner := syncRunner(probe)
	var recovered []string
	cycle := func(i int) (reEnabled, evidenceGated int) {
		pollTime := base.Add(time.Duration(i) * time.Second)
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		_, reEnabled, _, evidenceGated = runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{},
			now, probeCfg(), nil, func(p string) { recovered = append(recovered, p) }, runner)
		return reEnabled, evidenceGated
	}

	K := int(probeCfg().ReEnableHysteresis)
	for i := 1; i < K; i++ {
		reEnabled, evidenceGated := cycle(i)
		require.Zero(t, reEnabled)
		require.Equal(t, 1, evidenceGated, "a disabled endpoint holding evidence is reported as gated")
		require.Zero(t, probe.calls, "no replay before the poll hysteresis is earned")
		require.False(t, dc.Endpoint.Enabled)
	}
	// The K-th cycle replays the recorded request; the (inline) recovered verdict re-enables.
	reEnabled, _ := cycle(K)
	require.Zero(t, reEnabled, "replay-granted re-enables are applied by the replayer, not counted as the cycle's synchronous re-enable")
	require.Equal(t, 1, probe.calls, "exactly one replay at the threshold")
	require.Equal(t, "eth_call", probe.method)
	require.Equal(t, testEvidence, probe.payload, "the replay receives the exact recorded request")
	require.Equal(t, testEvidenceTimeout, probe.timeout, "the replay receives the recorded relay timeout")
	require.True(t, dc.Endpoint.Enabled)
	require.Equal(t, []string{"provider1"}, recovered, "a replay-confirmed recovery restores routing (F2)")
}

// TestProbeCycle_FailedReplayKeepsDisabledAndPacesRetries: a failing replay keeps the endpoint
// disabled, resets the streak, and escalates — the next replay happens only after the escalated
// K<<1 streak is re-earned, NOT on every cycle.
func TestProbeCycle_FailedReplayKeepsDisabledAndPacesRetries(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence, testEvidenceTimeout)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	probe := &relayProbeRecorder{verdicts: []relayProbeVerdict{relayProbeStillFailing, relayProbeRecovered}}
	runner := syncRunner(probe)
	cycle := func(i int) {
		pollTime := base.Add(time.Duration(i) * time.Second)
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{},
			now, probeCfg(), nil, nil, runner)
	}

	K := int(probeCfg().ReEnableHysteresis)
	for i := 1; i <= K; i++ {
		cycle(i)
	}
	require.Equal(t, 1, probe.calls, "one replay at the threshold, which failed")
	require.False(t, dc.Endpoint.Enabled, "a failing replay must not re-enable")

	// The next K cycles re-earn only the BASE streak — below the escalated K<<1 → no replay yet.
	for i := K + 1; i <= 2*K; i++ {
		cycle(i)
	}
	require.Equal(t, 1, probe.calls, "no replay until the escalated K<<1 streak is re-earned (pacing)")

	// Completing the escalated streak triggers the second replay, which passes → re-enabled.
	for i := 2*K + 1; i <= 3*K; i++ {
		cycle(i)
	}
	require.Equal(t, 2, probe.calls, "second replay exactly when the escalated streak completes")
	require.True(t, dc.Endpoint.Enabled)
}

// TestProbeCycle_InconclusiveReplayRetriesAtBaseStreak: an inconclusive replay paces the next
// attempt (streak reset) but does NOT escalate — the second replay is due after the BASE K streak,
// not K<<1. This is the observable difference between still-failing and inconclusive.
func TestProbeCycle_InconclusiveReplayRetriesAtBaseStreak(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence, testEvidenceTimeout)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	probe := &relayProbeRecorder{verdicts: []relayProbeVerdict{relayProbeInconclusive, relayProbeRecovered}}
	runner := syncRunner(probe)
	cycle := func(i int) {
		pollTime := base.Add(time.Duration(i) * time.Second)
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{},
			now, probeCfg(), nil, nil, runner)
	}

	K := int(probeCfg().ReEnableHysteresis)
	for i := 1; i <= K; i++ {
		cycle(i)
	}
	require.Equal(t, 1, probe.calls, "one replay at the threshold, inconclusive")
	require.False(t, dc.Endpoint.Enabled, "inconclusive must not re-enable")

	// No escalation: the BASE K streak earns the next replay, which passes.
	for i := K + 1; i <= 2*K; i++ {
		cycle(i)
	}
	require.Equal(t, 2, probe.calls, "inconclusive does not escalate — the second replay is due after the base K streak")
	require.True(t, dc.Endpoint.Enabled)
}

// TestProbeCycle_AsyncReplayDoesNotBlockTheCycle pins the review's scheduling complaint: the cycle
// must return while a slow replay is still running (the probe loop also feeds every provider's
// QoS), the verdict lands on the replayer's goroutine, and the single-flight guard keeps
// subsequent cycles from stacking a second replay of the same evidence.
func TestProbeCycle_AsyncReplayDoesNotBlockTheCycle(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence, testEvidenceTimeout)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	release := make(chan struct{})
	var calls atomic.Int32
	runner := &relayProbeRunner{
		async: true,
		probe: func(*lavasession.EndpointWithDirectConnection, string, []byte, time.Duration) relayProbeVerdict {
			calls.Add(1)
			<-release
			return relayProbeRecovered
		},
	}
	cycle := func(i int) {
		pollTime := base.Add(time.Duration(i) * time.Second)
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{},
			now, probeCfg(), nil, nil, runner)
	}

	K := int(probeCfg().ReEnableHysteresis)
	for i := 1; i <= K; i++ {
		cycle(i) // the K-th cycle launches the replay and MUST return while it blocks
	}
	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond,
		"the replay was launched off the cycle")
	require.False(t, dc.Endpoint.Enabled, "no verdict yet — the endpoint stays disabled while the replay runs")

	// Further cycles while the replay is outstanding: candidacy holds, but single-flight
	// suppresses a second launch.
	for i := K + 1; i <= K+3; i++ {
		cycle(i)
	}
	require.Equal(t, int32(1), calls.Load(), "one outstanding replay per endpoint, never stacked per tick")

	close(release)
	require.Eventually(t, func() bool { return dc.Endpoint.IsEnabled() }, time.Second, time.Millisecond,
		"the recovered verdict re-enables on the replayer's goroutine")
}

// TestRelayProbeRunner_SingleFlightPerKey: the guard is per endpoint key, released on completion.
func TestRelayProbeRunner_SingleFlightPerKey(t *testing.T) {
	r := &relayProbeRunner{async: true}
	started := make(chan struct{})
	release := make(chan struct{})
	var ran atomic.Int32

	r.launch("ep1", func() { close(started); <-release; ran.Add(1) })
	<-started
	r.launch("ep1", func() { ran.Add(1) }) // suppressed: ep1 already in flight
	r.launch("ep2", func() { ran.Add(1) }) // independent key: runs

	require.Eventually(t, func() bool { return ran.Load() == 1 }, time.Second, time.Millisecond, "only ep2 ran while ep1 was in flight")
	close(release)
	require.Eventually(t, func() bool { return ran.Load() == 2 }, time.Second, time.Millisecond)

	// ep1's slot is free again after completion.
	r.launch("ep1", func() { ran.Add(1) })
	require.Eventually(t, func() bool { return ran.Load() == 3 }, time.Second, time.Millisecond, "completion releases the key")
}

// TestProbeCycle_NilRelayProbeFallsBackToPollOnly: without replay wiring (nil replayer — tests,
// degraded setups) recorded evidence must not park the endpoint forever; the cycle falls back to
// poll-only evidence like the legacy path.
func TestProbeCycle_NilRelayProbeFallsBackToPollOnly(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false)
	dc.Endpoint.RecordFailingRelay("eth_call", testEvidence, testEvidenceTimeout)
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
	require.True(t, dc.Endpoint.Enabled, "nil replayer falls back to poll-only re-enable instead of parking the endpoint")
}

// ---------------------------------------------------------------------------
// replayFailingRelay — the judge
// ---------------------------------------------------------------------------

// fakeDirectConn is a scriptable DirectRPCConnection for the replay judge.
type fakeDirectConn struct {
	resp       *lavasession.DirectRPCResponse
	err        error
	gotPayload []byte
	gotTimeout time.Duration // remaining context budget observed by the request
}

func (f *fakeDirectConn) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	f.gotPayload = data
	if deadline, ok := ctx.Deadline(); ok {
		f.gotTimeout = time.Until(deadline)
	}
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

func replayWith(t *testing.T, conn *fakeDirectConn) relayProbeVerdict {
	t.Helper()
	// Hermetic hold-off state: a 429 case records into the registry, and without the
	// reset it would make every later case skip the probe as held-off.
	withFreshRelayHoldoff(t)
	rpcss := replayServer()
	epc := &lavasession.EndpointWithDirectConnection{
		Endpoint:         &lavasession.Endpoint{NetworkAddress: "http://fake:8545"},
		DirectConnection: conn,
		ProviderAddress:  "provider1",
	}
	return rpcss.replayFailingRelay(epc, "eth_call", testEvidence, testEvidenceTimeout)
}

// TestReplayFailingRelay_JudgesByHealthClassification pins the judge's three-way contract:
// "recovered" means the request no longer produces a HEALTH-AFFECTING failure — the same rule the
// relay path uses to mark endpoints unhealthy; "still-failing" means it still does; and anything
// the judge cannot classify (rate limits, unrecognized 4xx, unparseable bodies) is INCONCLUSIVE —
// never silently "recovered" (the fail-open the review flagged), never an escalating failure.
func TestReplayFailingRelay_JudgesByHealthClassification(t *testing.T) {
	cases := []struct {
		name string
		conn *fakeDirectConn
		want relayProbeVerdict
	}{
		{"clean 200 result", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}}, relayProbeRecovered},
		{"5xx still failing", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 503, Status: "503"}}, relayProbeStillFailing},
		{"429 proves nothing", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 429, Status: "429"}}, relayProbeInconclusive},
		{"501 is a capability answer, not a fault", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 501, Status: "501"}}, relayProbeRecovered},
		{"404 proves nothing (auth drift, proxy in the way)", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 404, Status: "404"}}, relayProbeInconclusive},
		{"401 proves nothing", &fakeDirectConn{err: &lavasession.HTTPStatusError{StatusCode: 401, Status: "401"}}, relayProbeInconclusive},
		{"transport error still failing", &fakeDirectConn{err: context.DeadlineExceeded}, relayProbeStillFailing},
		{"node internal error rides HTTP 200", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`)}}, relayProbeStillFailing},
		{"unsupported method in body is not a fault", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`)}}, relayProbeRecovered},
		{"unparseable 200 body (proxy HTML page) proves nothing", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`<html><body>Bad Gateway</body></html>`)}}, relayProbeInconclusive},
		{"empty 200 body proves nothing", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: nil}}, relayProbeInconclusive},
		{"batch reply (JSON array) cannot be judged", &fakeDirectConn{resp: &lavasession.DirectRPCResponse{
			StatusCode: 200, Data: []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`)}}, relayProbeInconclusive},
		{"nil response with nil error proves nothing", &fakeDirectConn{}, relayProbeInconclusive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, replayWith(t, tc.conn))
			require.Equal(t, testEvidence, tc.conn.gotPayload, "the judge sends the exact recorded bytes")
		})
	}
}

// TestReplayFailingRelay_UsesRecordedTimeout pins the review's timeout complaint: the replay's
// budget is the RECORDED relay timeout (what the request actually failed under), floored at
// minRelayProbeTimeout — never a tighter probe-only constant that would fail every
// slow-but-recovered heavy method forever.
func TestReplayFailingRelay_UsesRecordedTimeout(t *testing.T) {
	rpcss := replayServer()
	mkEp := func(conn *fakeDirectConn) *lavasession.EndpointWithDirectConnection {
		return &lavasession.EndpointWithDirectConnection{
			Endpoint:         &lavasession.Endpoint{NetworkAddress: "http://fake:8545"},
			DirectConnection: conn,
			ProviderAddress:  "provider1",
		}
	}
	okResp := func() *lavasession.DirectRPCResponse {
		return &lavasession.DirectRPCResponse{StatusCode: 200, Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}
	}

	// A heavy method recorded with a 25s relay budget is judged under ~25s.
	conn := &fakeDirectConn{resp: okResp()}
	rpcss.replayFailingRelay(mkEp(conn), "eth_getLogs", testEvidence, 25*time.Second)
	require.InDelta(t, (25 * time.Second).Seconds(), conn.gotTimeout.Seconds(), 2.0,
		"the replay context carries the recorded relay budget")

	// No recorded budget (0) floors at minRelayProbeTimeout, as does a shorter one.
	conn = &fakeDirectConn{resp: okResp()}
	rpcss.replayFailingRelay(mkEp(conn), "eth_call", testEvidence, 0)
	require.InDelta(t, minRelayProbeTimeout.Seconds(), conn.gotTimeout.Seconds(), 2.0,
		"a missing recorded budget falls back to the floor")

	conn = &fakeDirectConn{resp: okResp()}
	rpcss.replayFailingRelay(mkEp(conn), "eth_call", testEvidence, time.Second)
	require.InDelta(t, minRelayProbeTimeout.Seconds(), conn.gotTimeout.Seconds(), 2.0,
		"a recorded budget below the floor is raised to it")
}

func TestReplayFailingRelay_NoConnectionIsInconclusive(t *testing.T) {
	rpcss := replayServer()
	require.Equal(t, relayProbeInconclusive, rpcss.replayFailingRelay(nil, "eth_call", testEvidence, testEvidenceTimeout))
	require.Equal(t, relayProbeInconclusive, rpcss.replayFailingRelay(&lavasession.EndpointWithDirectConnection{}, "eth_call", testEvidence, testEvidenceTimeout))
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

// TestRecordRelayProbeEvidence_Eligibility pins the recording rules: single read-only JSON-RPC
// requests are recorded (with the relay path's effective timeout); writes (stateful),
// subscriptions, hanging APIs, batches, and non-JSON-RPC interfaces are not — those episodes fall
// back to the poll-only path with its trial budget. NOTE the gates are about request SHAPE, never
// failure CAUSE: 5xx storms and transport faults record like any other health-affecting failure.
func TestRecordRelayProbeEvidence_Eligibility(t *testing.T) {
	readOnly := &spectypes.Api{Name: "eth_call"}
	write := &spectypes.Api{Name: "eth_sendRawTransaction", Category: spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS}}
	hanging := &spectypes.Api{Name: "eth_newFilter", Category: spectypes.SpecCategory{HangingApi: true}}
	subscribe := &spectypes.Api{Name: "eth_subscribe"}
	subscribeDirective := &spectypes.ParseDirective{ApiName: "eth_subscribe", FunctionTag: spectypes.FUNCTION_TAG_SUBSCRIBE}
	batched := &spectypes.Api{Name: "eth_call" + chainlib.SEP + "eth_getLogs"}

	cases := []struct {
		name         string
		apiInterface string
		msg          chainlib.ChainMessage
		payload      []byte
		wantRecorded bool
	}{
		{"read-only jsonrpc method is recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, readOnly, nil), testEvidence, true},
		{"write method never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, write, nil), testEvidence, false},
		{"hanging api never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, hanging, nil), testEvidence, false},
		{"subscription never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, subscribe, subscribeDirective), testEvidence, false},
		{"non-jsonrpc interface never recorded", spectypes.APIInterfaceRest, mockMessage(t, readOnly, nil), testEvidence, false},
		{"batch (JSON array body) never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, batched, nil),
			[]byte(`[{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}]`), false},
		{"batch with leading whitespace never recorded", spectypes.APIInterfaceJsonRPC, mockMessage(t, batched, nil),
			[]byte("  \n\t[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[]}]"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rpcss := &RPCSmartRouterServer{
				listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: tc.apiInterface},
			}
			endpoint := &lavasession.Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
			rpcss.recordRelayProbeEvidence(endpoint, tc.msg, tc.payload, testEvidenceTimeout)
			recorded := endpoint.HealthSnapshot().RelayProbeMethod != ""
			require.Equal(t, tc.wantRecorded, recorded)
		})
	}
}
