package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// A subscribe the node refuses, and an error frame it sends on an open subscription, are node
// errors the router counts like any other (PRD §5.7; MAG-5598): each builds an error record,
// takes its verdict through the classification order (§3.2), and is counted in
// smartrouter_errors_total with source="subscription" and in relay_end. Nothing else about the
// subscription changes: selection, retry, restore, teardown and the url's health stay as they were
// (every refusal but a 429 counts a failure, noteSubscribeFailure; an error frame ends the
// subscription with no count). Connect, upgrade and transport failures keep the connection rules
// and are not counted here.

// subscriptionNodeErrorVerdict is the order's verdict on nodeErr: the JSON-RPC error a node answered
// a subscription request with, or pushed on an open subscription. The record is the one the
// JSON-RPC reader builds from that error as a reply; the chain's policy is nil until spec rows act
// (MAG-5606).
func subscriptionNodeErrorVerdict(chainID, apiInterface, method string, isWrite bool, nodeErr *rpcclient.JsonError) *common.RouterError {
	record := common.ErrorRecord{Interface: apiInterface, Status: http.StatusOK, Text: nodeErr.Message}
	body, err := json.Marshal(struct {
		JSONRPC string               `json:"jsonrpc"`
		ID      int                  `json:"id"`
		Error   *rpcclient.JsonError `json:"error"`
	}{JSONRPC: "2.0", ID: 1, Error: nodeErr})
	if err == nil {
		if _, records := common.ReadJSONRPCReply(apiInterface, http.StatusOK, body); len(records) > 0 {
			record = records[0]
		}
	}
	verdict := common.ClassifyInOrder(nil, common.GetChainFamilyOrDefault(chainID), record, common.ReplyContext{Method: method, IsWrite: isWrite})
	return verdict.Classified()
}

// countSubscriptionNodeError counts nodeErr in smartrouter_errors_total with source="subscription"
// and returns its verdict, for relay_end to write the same name.
func countSubscriptionNodeError(chainID, apiInterface, method string, isWrite bool, nodeErr *rpcclient.JsonError) *common.RouterError {
	routerError := subscriptionNodeErrorVerdict(chainID, apiInterface, method, isWrite, nodeErr)
	common.EmitErrorMetric(routerError, chainID, common.ErrorOrigin{APIInterface: apiInterface, Source: common.ErrorSourceSubscription})
	return routerError
}

// subscriptionErrorFrameReporter writes the relay_end of an error frame a node sent on an open
// subscription: method is the subscribe method, upstream the subscription's provider.
type subscriptionErrorFrameReporter func(method, upstream string, nodeErr *rpcclient.JsonError, routerError *common.RouterError)

// reportSubscriptionErrorFrame is the server's subscriptionErrorFrameReporter. The frame answers no
// request in flight, so the event names no GUID: it is the subscription's provider ending it with a
// node error, one attempt that reached the client as the subscription's end.
func (rpcss *RPCSmartRouterServer) reportSubscriptionErrorFrame(method, upstream string, nodeErr *rpcclient.JsonError, routerError *common.RouterError) {
	emitRelayEnd(rpcss.buildSubscriptionErrorFrameRelayEnd(method, upstream, nodeErr, routerError))
}

func (rpcss *RPCSmartRouterServer) buildSubscriptionErrorFrameRelayEnd(method, upstream string, nodeErr *rpcclient.JsonError, routerError *common.RouterError) *relayEnd {
	end := rpcss.newRelayEnd(context.Background(), nil, time.Now(), nodeErr)
	end.selection = relayEndSelectionSubscription
	end.method = relayEndMethod(method, false)
	end.outcome = relayOutcomeNodeError
	end.servedBy = upstream
	end.errorCode = relayEndCode(routerError)
	end.errorText = relayEndText(nodeErr.Message)
	end.attempts = relayEndAttempts{{
		upstream:         upstream,
		outcome:          attemptOutcomeNodeError,
		errorCode:        relayEndCode(routerError),
		message:          relayEndText(nodeErr.Message),
		nodeCode:         nodeErr.Code,
		returnedToClient: true,
		routerError:      routerError,
	}}
	return end
}
