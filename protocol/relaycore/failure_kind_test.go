package relaycore

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// sendNodeReply records an upstream's reply that the router reads as a node error: its status and
// body, with the flags the sender's classification set on it.
func sendNodeReply(rp *RelayProcessor, provider string, delay time.Duration, status int, body string, flags func(*common.RelayResult)) {
	time.Sleep(delay)
	rp.GetUsedProviders().RemoveUsed(provider, routersession.NewRouterKey(nil), nil)
	result := common.RelayResult{
		Request: &pairingtypes.RelayRequest{
			RelaySession: &pairingtypes.RelaySession{},
			RelayData:    &pairingtypes.RelayPrivateData{},
		},
		Reply:        &pairingtypes.RelayReply{Data: []byte(body)},
		ProviderInfo: common.ProviderInfo{ProviderAddress: provider},
		StatusCode:   status,
		IsNodeError:  true,
	}
	if flags != nil {
		flags(&result)
	}
	rp.SetResponse(&RelayResponse{RelayResult: result})
}

// upstream429Err is a vendor's 429 as the JSON-RPC sender records it.
func vendor429Err(body string) error {
	return upstream429RelayError(body).Err
}

// When every provider failed, the reply is the failure of the best kind, not the first to arrive
// (Part I R-K, MAG-5061 item 2, MAG-5232 Expected 8): C2 > C3 > C5 / unknown > C4 > C6. A 403 is
// about the router's key, a 429 about its rate, and a rate limit passes on its own: the client
// gets the 429 even when the 403 arrives first. On main the node error always won, so the 403 did.
func TestProcessingResult_A429BeatsA403(t *testing.T) {
	for _, refusalFirst := range []bool{true, false} {
		refusalDelay, throttleDelay := time.Millisecond, 30*time.Millisecond
		if !refusalFirst {
			refusalDelay, throttleDelay = throttleDelay, refusalDelay
		}
		rp, _, closer := newTwoProviderProcessor(t)
		go sendNodeReply(rp, "provider@a", refusalDelay, http.StatusForbidden, `{"message":"key disabled"}`, nil)
		go SendProtocolError(rp, "provider@b", throttleDelay, vendor429Err(`{"error":"quota"}`))
		waitForBoth(t, rp)

		result, err := rp.ProcessingResult()
		closer()
		require.Error(t, err, "refusal first: %v", refusalFirst)
		require.Equal(t, http.StatusTooManyRequests, result.StatusCode, "refusal first: %v", refusalFirst)
		require.Equal(t, `{"error":"quota"}`, string(result.GetClientFacing().UpstreamBody))
	}
}

// A provider fault (C5) beats a refusal (C6): the 503 is about the request's provider, the 403
// about the router's key.
func TestProcessingResult_AProviderFaultBeatsARefusal(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()
	go sendNodeReply(rp, "provider@a", time.Millisecond, http.StatusForbidden, `{"message":"key disabled"}`, nil)
	go SendProtocolError(rp, "provider@b", 30*time.Millisecond, upstream503Err("node b down"))
	waitForBoth(t, rp)

	result, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, result.StatusCode)
	require.Equal(t, "node b down", string(result.GetClientFacing().UpstreamBody))
}

// The caller's mistake (C2) and "not here" (C3) beat a provider fault (C5), and a provider fault
// beats a throttle (C4), wherever the failure came from: a node error or an upstream's status.
func TestProcessingResult_RanksNodeErrorsAndUpstreamStatusesTogether(t *testing.T) {
	callersMistake := func(r *common.RelayResult) { r.IsNonRetryable = true }
	notHere := func(r *common.RelayResult) { r.IsDataScope = true }
	rateLimited := func(r *common.RelayResult) { r.IsRateLimited = true }

	for _, c := range []struct {
		name       string
		nodeStatus int
		nodeFlags  func(*common.RelayResult)
		upstream   error
		wantNode   bool
	}{
		{"the caller's mistake beats a 503", http.StatusBadRequest, callersMistake, upstream503Err("down"), true},
		{"not here beats a 503", http.StatusNotFound, notHere, upstream503Err("down"), true},
		{"a 503 beats a throttle in a node's reply", http.StatusInternalServerError, rateLimited, upstream503Err("down"), false},
		{"a provider fault in a node's reply beats a 429", http.StatusInternalServerError, nil, vendor429Err("quota"), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			rp, _, closer := newTwoProviderProcessor(t)
			defer closer()
			// The upstream's status arrives first: arrival order must not decide.
			go SendProtocolError(rp, "provider@b", time.Millisecond, c.upstream)
			go sendNodeReply(rp, "provider@a", 30*time.Millisecond, c.nodeStatus, `{"message":"node a"}`, c.nodeFlags)
			waitForBoth(t, rp)

			result, _ := rp.ProcessingResult()
			require.NotNil(t, result)
			if c.wantNode {
				require.Equal(t, "provider@a", result.ProviderInfo.ProviderAddress)
				require.Equal(t, `{"message":"node a"}`, string(result.Reply.Data))
				return
			}
			require.NotNil(t, result.GetClientFacing())
			require.Equal(t, http.StatusServiceUnavailable, result.StatusCode)
		})
	}
}

// failureKind reads the kind off the status first: whatever the classifier made of the body, a
// 401, 402 or 403 is the provider refusing the router, and a 429 is the provider limiting it.
func TestFailureKind(t *testing.T) {
	for _, c := range []struct {
		name   string
		status int
		result common.RelayResult
		router *common.RouterError
		want   failureKind
	}{
		{"401", http.StatusUnauthorized, common.RelayResult{IsNodeCapability: true}, common.RouterErrorNodeUnauthorized, kindRefusesUs},
		{"402", http.StatusPaymentRequired, common.RelayResult{}, nil, kindRefusesUs},
		{"403 the classifier read as the caller's mistake", http.StatusForbidden, common.RelayResult{IsNonRetryable: true}, nil, kindRefusesUs},
		{"credentials refused inside a 200", http.StatusOK, common.RelayResult{IsNodeCapability: true}, common.RouterErrorNodeAccessDenied, kindRefusesUs},
		{"429", http.StatusTooManyRequests, common.RelayResult{}, nil, kindThrottled},
		{"rate limited inside a 200", http.StatusOK, common.RelayResult{IsRateLimited: true}, nil, kindThrottled},
		{"the caller's mistake", http.StatusOK, common.RelayResult{IsNonRetryable: true}, nil, kindCallersMistake},
		{"data not held", http.StatusOK, common.RelayResult{IsDataScope: true}, nil, kindCannotServeHere},
		{"method not served here", http.StatusOK, common.RelayResult{IsNodeCapability: true}, common.RouterErrorNodeMethodNotFound, kindCannotServeHere},
		{"a provider fault", http.StatusServiceUnavailable, common.RelayResult{IsNodeAtFault: true}, nil, kindProviderFault},
		{"unclassified", http.StatusOK, common.RelayResult{}, nil, kindProviderFault},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, failureKindOf(c.status, c.router, c.result))
		})
	}
	require.Greater(t, kindCallersMistake, kindCannotServeHere)
	require.Greater(t, kindCannotServeHere, kindProviderFault)
	require.Greater(t, kindProviderFault, kindThrottled)
	require.Greater(t, kindThrottled, kindRefusesUs)
}
