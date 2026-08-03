package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// This file is the MAG-2550 recovery-probe pair: the relay path RECORDS the failing read-only
// request when an endpoint is marked unhealthy (recordRelayProbeEvidence), and the probe loop
// REPLAYS it before re-enabling the disabled endpoint (replayFailingRelay). Together they close
// the evidence gap that caused the disable/re-enable flap: the poll method (eth_blockNumber) and
// the failing relay method (e.g. eth_call) measure different things, so healthy polls alone must
// never re-enable an endpoint whose relay path was the thing that broke.

// relayProbeTimeout bounds one replay of the recorded failing request. Deliberately generous
// relative to the probe cadence: the replay runs at most once per earned poll-hysteresis streak
// per disabled endpoint, off the data plane, and a slow-but-correct answer is still recovery
// evidence.
const relayProbeTimeout = 10 * time.Second

// relayProbeFunc replays a disabled endpoint's recorded failing request and reports whether it no
// longer produces a health-affecting failure. Injected into runProbeCycleCore so the pure cycle
// body stays testable without network I/O.
type relayProbeFunc func(ep *lavasession.EndpointWithDirectConnection, method string, payload []byte) bool

// recordRelayProbeEvidence stores the failing request on the endpoint as replayable recovery
// evidence (Endpoint.RecordFailingRelay), called just before MarkUnhealthy on a health-affecting
// relay failure. Eligibility gates, in order:
//   - JSON-RPC interface only: the raw POST body is fully self-contained, so replaying the exact
//     bytes through DirectRPCConnection.SendRequest reproduces the request faithfully. REST/gRPC
//     requests carry out-of-band routing (URL path, method descriptors) and fall back to the
//     poll-only re-enable path with its trial budget.
//   - Read-only methods only: a write (Stateful == CONSISTENCY_SELECT_ALL_PROVIDERS) must never be
//     re-broadcast as a health check, subscriptions don't fit a one-shot replay, and hanging APIs
//     would stall the probe cycle. Writes are also broadcast to all providers on the client path,
//     so a write-broken provider cannot single-handedly fail a client anyway.
//
// Unsupported-method responses never reach this function at all: classifyEndpointHealth returns
// shouldMarkUnhealthy=false for them, so they neither disable the endpoint nor pollute the
// recorded evidence.
func (rpcss *RPCSmartRouterServer) recordRelayProbeEvidence(endpoint *lavasession.Endpoint, chainMessage chainlib.ChainMessage, requestData []byte) {
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
	endpoint.RecordFailingRelay(chainMessage.GetApi().Name, requestData)
}

// replayFailingRelay sends the recorded failing request through the endpoint's own direct
// connection and judges the outcome by the SAME classification rules the relay path uses to mark
// endpoints unhealthy. "Success" therefore means exactly: this request no longer produces a
// health-affecting failure. A user-level node error (e.g. an execution revert) counts as
// recovered — the transport and the method handler answered — while 5xx, timeouts, connection
// errors, and health-affecting node errors keep the endpoint disabled. A 429 is treated as
// failure: a rate-limited replay never exercised the failing handler, so it proves nothing either
// way and the probe simply retries after the next earned poll streak.
func (rpcss *RPCSmartRouterServer) replayFailingRelay(ep *lavasession.EndpointWithDirectConnection, method string, payload []byte) bool {
	if ep == nil || ep.DirectConnection == nil || len(payload) == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), relayProbeTimeout)
	defer cancel()
	resp, err := ep.DirectConnection.SendRequest(ctx, payload, nil)

	chainFamily := common.ChainFamily(-1)
	if family, ok := common.GetChainFamily(rpcss.listenEndpoint.ChainID); ok {
		chainFamily = family
	}

	if err != nil {
		// HTTP-status errors mirror the relay path's status rule (5xx unhealthy except 501,
		// which is a capability answer, not a fault).
		if httpErr, ok := err.(*lavasession.HTTPStatusError); ok {
			if httpErr.StatusCode == http.StatusTooManyRequests {
				return false // busy: the failing handler was never exercised — no evidence
			}
			unhealthy, _ := classifyHTTPStatus(httpErr.StatusCode)
			return !unhealthy || httpErr.StatusCode == http.StatusNotImplemented
		}
		classified, _ := classifyDirectRPCError(err, chainFamily, common.TransportJsonRPC)
		if classified.IsRateLimited() {
			return false
		}
		unhealthy, _ := classifyEndpointHealth(classified, false)
		return !unhealthy
	}
	if resp == nil {
		return false
	}

	// Transport-level success. A JSON-RPC node error can still ride an HTTP 200 body — classify it
	// with the same registry the relay path uses. Batch replies (JSON arrays) don't match the
	// single-object shape and fall through to success: the transport answered, which is all a
	// multi-request payload can prove per-replay.
	var nodeErr struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if len(resp.Data) > 0 && json.Unmarshal(resp.Data, &nodeErr) == nil && nodeErr.Error != nil {
		classified := common.ClassifyError(nil, chainFamily, common.TransportJsonRPC, nodeErr.Error.Code, nodeErr.Error.Message)
		if classified.IsRateLimited() {
			return false
		}
		unhealthy, _ := classifyEndpointHealth(classified, false)
		return !unhealthy
	}
	return true
}
