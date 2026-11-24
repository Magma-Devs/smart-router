package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// TestRelayInnerDirect_GraphQLReadVerdictDecidesUnderEvery5xx pins that a GraphQL read's verdict,
// read from the body, is what decides a 5xx reply end to end, under every 5xx (MAG-5121). The
// status step that marks an endpoint unhealthy by its status alone used to decide a JSON reply
// under 502, 504, 505, 507, 522, 524 and 599 again, after the verdict: a 502 carrying the caller's
// BAD_USER_INPUT failed the relay and switched the endpoint off, where the same body under a 500 or
// a 503 was served as is.
func TestRelayInnerDirect_GraphQLReadVerdictDecidesUnderEvery5xx(t *testing.T) {
	const (
		read     = `{"query":"{ chainIdentifier }"}`
		mutation = `{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`
		caller   = `{"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}}]}`
		feature  = `{"errors":[{"message":"not here","extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`
		fault    = `{"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`
		page     = `<html><body>502 Bad Gateway</body></html>`
	)
	chainParser := newGatewayTestGraphQLParser(t)

	tests := []struct {
		name    string
		request string
		status  int
		body    string
		// wantReply: the relay ends with the provider's reply (served as is, or retried
		// elsewhere by its flags), not a router-made transport error.
		wantReply      bool
		wantNonRetry   bool
		wantStillOn    bool
		wantNodeAtFult bool
	}{
		{"a 502 carrying the caller's error is served as is, not blamed", read, http.StatusBadGateway, caller, true, true, true, false},
		{"a 504 carrying the caller's error is served as is, not blamed", read, http.StatusGatewayTimeout, caller, true, true, true, false},
		{"a 599 carrying the caller's error is served as is, not blamed", read, 599, caller, true, true, true, false},
		{"a 504 carrying FEATURE_UNAVAILABLE goes elsewhere, not blamed", read, http.StatusGatewayTimeout, feature, true, false, true, false},
		{"a 502 carrying INTERNAL_SERVER_ERROR is still blamed", read, http.StatusBadGateway, fault, true, false, false, true},
		{"a 502 page is still blamed", read, http.StatusBadGateway, page, true, false, false, true},
		{"control: a 500 carrying the caller's error was already served as is", read, http.StatusInternalServerError, caller, true, true, true, false},
		// Writes still stand on the shared rows (MAG-5122): the status step decides them as before.
		{"a write's 502 is still decided by its status", mutation, http.StatusBadGateway, caller, false, false, false, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			benchAfter(t, 1)
			h := newDirectRelayHarness(t, test.status, test.body)
			h.rpcss.listenEndpoint = &routersession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL}

			chainMessage, err := chainParser.ParseMsg("", []byte(test.request), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			relayResult := &common.RelayResult{}
			_, relayErr := h.rpcss.relayInnerDirect(
				context.Background(), h.session, relayResult, 5*time.Second, 30*time.Second,
				chainMessage, []byte(test.request), func() bool { return false }, nil,
				false, // not the router's health check
			)

			if test.wantReply {
				require.NoError(t, relayErr, "the provider's reply goes on as the relay's result")
				require.NotNil(t, relayResult.Reply)
				require.Equal(t, test.body, string(relayResult.Reply.Data), "the reply is kept as is")
				require.True(t, relayResult.IsNodeError)
				require.Equal(t, test.wantNonRetry, relayResult.IsNonRetryable, "served as is (not retried) only for the caller's error")
				require.Equal(t, test.wantNodeAtFult, relayResult.IsNodeAtFault)
			} else {
				require.Error(t, relayErr, "the status step fails the relay")
			}
			require.Equal(t, test.wantStillOn, h.endpoint.IsEnabled(), "the endpoint is switched off only when the verdict blames it")
		})
	}
}
