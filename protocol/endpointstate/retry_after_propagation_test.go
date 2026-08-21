package endpointstate

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The poll path builds HTTPStatusError with the parsed Retry-After, then used to flatten
// it into a FormatDebug attribute one frame up — a plain string error with no Unwrap.
// This pins the fix: the typed 429 and its Retry-After must survive FetchLatestBlockNum's
// wrapping so the ChainTracker's backoff can read them.
func TestEndpointPoller_FetchLatestBlockNum_PreservesRateLimit(t *testing.T) {
	chainParser := newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)

	url := "http://eth-ep:8545"
	conn := &mockDirectRPCConnection{
		url: url,
		sendErr: &routersession.HTTPStatusError{
			StatusCode: 429,
			Status:     "429",
			RetryAfter: 90 * time.Second,
		},
	}
	poller := NewEndpointPoller(
		&routersession.Endpoint{NetworkAddress: url, Enabled: true},
		conn,
		chainParser,
		"ETH1",
		spectypes.APIInterfaceJsonRPC,
	)

	_, err := poller.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	d, ok := common.RetryAfterFrom(err)
	require.True(t, ok, "Retry-After must survive the poller's wrapping")
	require.Equal(t, 90*time.Second, d)
}

// A non-429 upstream rejection through the same path must not read as a rate limit.
func TestEndpointPoller_FetchLatestBlockNum_ServerErrorIsNotRateLimit(t *testing.T) {
	chainParser := newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)

	url := "http://eth-ep:8545"
	conn := &mockDirectRPCConnection{
		url:     url,
		sendErr: &routersession.HTTPStatusError{StatusCode: 503, Status: "503"},
	}
	poller := NewEndpointPoller(
		&routersession.Endpoint{NetworkAddress: url, Enabled: true},
		conn,
		chainParser,
		"ETH1",
		spectypes.APIInterfaceJsonRPC,
	)

	_, err := poller.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok)
}
