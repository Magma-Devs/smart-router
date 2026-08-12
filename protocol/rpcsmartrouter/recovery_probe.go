package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// This file is the MAG-2550 recovery-probe pair: the relay path RECORDS the failing read-only
// request when an endpoint is marked unhealthy (recordRelayProbeEvidence), and the probe loop
// REPLAYS it before re-enabling the disabled endpoint (replayFailingRelay). Together they close
// the evidence gap that caused the disable/re-enable flap: the poll method (eth_blockNumber) and
// the failing relay method (e.g. eth_call) measure different things, so healthy polls alone must
// never re-enable an endpoint whose relay path was the thing that broke.

// minRelayProbeTimeout floors one replay's budget. The effective budget is the RECORDED relay
// timeout — the spec/CU-derived budget the request actually failed under, carried with the
// evidence — never less than this floor, and extended by the endpoint's own NodeUrl.Timeout
// exactly like the relay path (LowerContextTimeoutWithDuration). A flat probe timeout below the
// relay path's would fail every slow-but-recovered heavy method (eth_getLogs, debug_trace*)
// forever, holding a healthy endpoint out of rotation until the epoch reset.
const minRelayProbeTimeout = 10 * time.Second

// relayProbeVerdict is the replay judge's three-way outcome. The state machine treats them very
// differently: recovered completes the two-step re-enable; still-failing resets the streak AND
// escalates the flap hysteresis (a relay-path failure was demonstrated); inconclusive resets the
// streak WITHOUT escalating (nothing was demonstrated either way). Both non-recovered verdicts
// consume one replay attempt of the evidence, which is dropped after maxRelayProbeAttempts —
// the bounded escape to the poll-only path.
type relayProbeVerdict int

const (
	relayProbeRecovered relayProbeVerdict = iota
	relayProbeStillFailing
	relayProbeInconclusive
)

// relayProbeFunc replays a disabled endpoint's recorded failing request under the recorded relay
// timeout and judges the outcome. Injected into runProbeCycleCore (via relayProbeRunner) so the
// pure cycle body stays testable without network I/O.
type relayProbeFunc func(ep *routersession.EndpointWithDirectConnection, method string, payload []byte, relayTimeout time.Duration) relayProbeVerdict

// relayProbeRunner executes replays for the probe cycle WITHOUT blocking it. The probe loop is a
// single serial ticker that also feeds every provider's QoS sample; a replay can legitimately run
// tens of seconds (heavy recorded methods), and a provider-wide recovery makes several endpoints
// replay candidates in the same cycle — run inline they would serialize to N×timeout, starving
// the QoS feed and silently dropping ticks. So launch() runs each replay on its own goroutine
// (async=false runs inline, for deterministic tests) and applies the verdict on completion via
// the endpoint's own thread-safe transitions (ConfirmRelayRecovery / RelayProbeFailed /
// RelayProbeInconclusive, all inert against racing re-enables). inFlight guarantees at most ONE
// outstanding replay per endpoint: candidacy is re-derived every cycle while the streak holds, so
// without the guard each 5s tick would stack another replay of the same evidence.
type relayProbeRunner struct {
	probe relayProbeFunc
	async bool

	mu       sync.Mutex
	inFlight map[string]struct{} // keyed by Endpoint.NetworkAddress
}

// launch runs one replay for the endpoint keyed by key, unless one is already outstanding.
func (r *relayProbeRunner) launch(key string, run func()) {
	r.mu.Lock()
	if _, busy := r.inFlight[key]; busy {
		r.mu.Unlock()
		return
	}
	if r.inFlight == nil {
		r.inFlight = make(map[string]struct{})
	}
	r.inFlight[key] = struct{}{}
	r.mu.Unlock()

	wrapped := func() {
		defer func() {
			r.mu.Lock()
			delete(r.inFlight, key)
			r.mu.Unlock()
		}()
		run()
	}
	if r.async {
		go wrapped()
		return
	}
	wrapped()
}

// recordRelayProbeEvidence stores the failing request on the endpoint as replayable recovery
// evidence (Endpoint.RecordFailingRelay), called just before MarkUnhealthy on a health-affecting
// relay failure. relayTimeout is the relay path's effective budget for this request, recorded so
// the replay judges under the same budget the request failed with.
//
// NOTE the eligibility gates below are about the SHAPE of the request, never the CAUSE of the
// failure: any health-affecting failure of an eligible request records evidence — 5xx storms,
// timeouts, and transport faults included (they all carry a valid chainMessage). On a JSON-RPC
// router this makes recording the COMMON disable path; the poll-only fallback is the exception
// (REST/gRPC interfaces, writes, subscriptions, hanging APIs, batches, oversized payloads) plus
// episodes whose evidence was dropped after maxRelayProbeAttempts non-recovered replays.
// Eligibility gates, in order:
//   - JSON-RPC interface only: the raw POST body is fully self-contained, so replaying the exact
//     bytes through DirectRPCConnection.SendRequest reproduces the request faithfully. REST/gRPC
//     requests carry out-of-band routing (URL path, method descriptors) and fall back to the
//     poll-only re-enable path with its trial budget.
//   - Read-only methods only: a write (Stateful == CONSISTENCY_SELECT_ALL_PROVIDERS) must never be
//     re-broadcast as a health check, subscriptions don't fit a one-shot replay, and hanging APIs
//     would stall the probe cycle. Writes are also broadcast to all providers on the client path,
//     so a write-broken provider cannot single-handedly fail a client anyway.
//   - No batches (top-level JSON arrays): a batch reply cannot be judged by the single-response
//     health classification, so a batch replay could only ever prove transport success — no more
//     than the poll it is meant to improve on. Recording it would make the gate a silent no-op;
//     falling back to the poll-only path says what it means. (This also keeps the synthesized
//     unbounded a+SEP+b batch method name out of the evidence — see 6fef8e0 for the metrics-label
//     twin of that problem.)
//
// Unsupported-method responses never reach this function at all: classifyEndpointHealth returns
// shouldMarkUnhealthy=false for them, so they neither disable the endpoint nor pollute the
// recorded evidence.
func (rpcss *RPCSmartRouterServer) recordRelayProbeEvidence(endpoint *routersession.Endpoint, chainMessage chainlib.ChainMessage, requestData []byte, relayTimeout time.Duration) {
	if endpoint == nil || chainMessage == nil {
		return
	}
	if rpcss.listenEndpoint.ApiInterface != spectypes.APIInterfaceJsonRPC {
		return
	}
	if chainlib.GetStateful(chainMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
		return
	}
	if chainlib.IsHangingApi(chainMessage) {
		return
	}
	if chainlib.IsFunctionTagOfType(chainMessage, spectypes.FUNCTION_TAG_SUBSCRIBE) {
		return
	}
	if isBatchPayload(requestData) {
		return
	}
	endpoint.RecordFailingRelay(chainMessage.GetApi().Name, requestData, relayTimeout)
}

// isBatchPayload reports whether the raw JSON-RPC POST body is a batch — a top-level JSON array.
func isBatchPayload(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return b == '['
		}
	}
	return false
}

// replayFailingRelay sends the recorded failing request through the endpoint's own direct
// connection and judges the outcome by the SAME classification rules the relay path uses to mark
// endpoints unhealthy. The budget mirrors the relay path too: the recorded relay timeout (floored
// at minRelayProbeTimeout) extended by the endpoint's NodeUrl.Timeout override, so a request that
// was failing under N seconds is judged under N seconds — never a tighter probe-only deadline.
//
// The three verdicts:
//   - recovered: the request no longer produces a health-affecting failure. A user-level node
//     error (e.g. an execution revert) counts — the transport and the method handler answered.
//   - still-failing: a health-affecting failure the relay path would disable on (5xx except 501,
//     timeouts, connection errors, health-affecting node errors).
//   - inconclusive: the replay proved nothing either way — rate-limited (the failing handler was
//     never exercised), an unclassifiable HTTP status (auth drift, proxy 4xx), or an unparseable
//     body on transport success (a proxy's HTML error page). Failing OPEN here would degrade the
//     gate to "always confirm" with no signal; failing CLOSED would escalate the flap hysteresis
//     on evidence of nothing. Inconclusive does neither.
func (rpcss *RPCSmartRouterServer) replayFailingRelay(ep *routersession.EndpointWithDirectConnection, method string, payload []byte, relayTimeout time.Duration) relayProbeVerdict {
	if ep == nil || ep.DirectConnection == nil || len(payload) == 0 {
		return relayProbeInconclusive
	}
	if relayTimeout < minRelayProbeTimeout {
		relayTimeout = minRelayProbeTimeout
	}
	ctx, cancel := ep.DirectConnection.GetNodeUrl().LowerContextTimeoutWithDuration(context.Background(), relayTimeout)
	defer cancel()
	resp, err := ep.DirectConnection.SendRequest(ctx, payload, nil)

	chainFamily := common.ChainFamily(-1)
	if family, ok := common.GetChainFamily(rpcss.listenEndpoint.ChainID); ok {
		chainFamily = family
	}

	if err != nil {
		// HTTP-status errors mirror the relay path's status rule (5xx unhealthy except 501,
		// which is a capability answer, not a fault). Statuses the relay path would NOT disable
		// on (unrecognized 4xx: auth drift, a proxy in the way) prove nothing about the recorded
		// failure — inconclusive, never "recovered".
		if httpErr, ok := err.(*routersession.HTTPStatusError); ok {
			switch {
			case httpErr.StatusCode == http.StatusTooManyRequests:
				return relayProbeInconclusive // busy: the failing handler was never exercised
			case httpErr.StatusCode == http.StatusNotImplemented:
				return relayProbeRecovered // capability answer, not a fault
			default:
				if unhealthy, _ := classifyHTTPStatus(httpErr.StatusCode); unhealthy {
					return relayProbeStillFailing
				}
				return relayProbeInconclusive
			}
		}
		classified, _ := classifyDirectRPCError(err, chainFamily, common.TransportJsonRPC)
		if classified.IsRateLimited() {
			return relayProbeInconclusive
		}
		if unhealthy, _ := classifyEndpointHealth(classified, false); unhealthy {
			return relayProbeStillFailing
		}
		// The relay path would not have disabled on this error — the health-affecting failure is
		// gone, which is exactly what the gate needs to know.
		return relayProbeRecovered
	}
	if resp == nil {
		return relayProbeInconclusive
	}

	// Transport-level success. A JSON-RPC node error can still ride an HTTP 200 body — classify it
	// with the same registry the relay path uses. A body that does not parse as a single JSON-RPC
	// object (empty, a proxy's HTML error page, a stray batch array — batches are never recorded)
	// cannot be judged: inconclusive, not success.
	var nodeErr struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if len(resp.Data) == 0 || json.Unmarshal(resp.Data, &nodeErr) != nil {
		return relayProbeInconclusive
	}
	if nodeErr.Error != nil {
		classified := common.ClassifyError(nil, chainFamily, common.TransportJsonRPC, nodeErr.Error.Code, nodeErr.Error.Message)
		if classified.IsRateLimited() {
			return relayProbeInconclusive
		}
		if unhealthy, _ := classifyEndpointHealth(classified, false); unhealthy {
			return relayProbeStillFailing
		}
	}
	return relayProbeRecovered
}
