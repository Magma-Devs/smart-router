package rpcsmartrouter

import (
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// Detection is structural wherever a status code exists: ValidateStatusCodes mints
// common.StatusCodeError429 and every HTTP-family proxy propagates it as the Format wrapper's cause,
// so errors.Is finds it through the wrapping. The cases below drive each transport the way
// production produces it, so a path that starts discarding its typed error fails here rather
// than silently demoting a healthy provider.
func TestIsRateLimitFailure_ProductionShapes(t *testing.T) {
	rateLimited := []struct {
		name string
		err  error
	}{
		{
			"the bare sentinel",
			common.StatusCodeError429,
		},
		{
			// jsonrpc over http: ValidateStatusCodes mints the sentinel, the proxy wraps it.
			"jsonrpc over http, one wrap",
			utils.FormatWarning("Received invalid status code", common.StatusCodeError429),
		},
		{
			// rest / tendermint: same sentinel, reached through HandleStatusError. These two
			// proxies used to pass nil here and render the code into an attribute instead,
			// which left nothing for errors.Is to find.
			"rest, sentinel through the full verify stack",
			utils.FormatWarning("[-] verify failed sending chainMessage",
				utils.FormatWarning("Received invalid status code", common.StatusCodeError429)),
		},
		{
			// The latest-block path, which used to drop the cause into an attribute.
			"wrapped sentinel via the GET_BLOCKNUM path",
			utils.FormatWarning("GET_BLOCKNUM failed sending chainMessage", common.StatusCodeError429),
		},
		{
			// grpc is the one transport with no status code to check: grpc-go reports
			// codes.Unavailable and the 429 survives only inside the status description.
			"grpc transport, no status code exists",
			errors.New(`[-] verify failed sending chainMessage ErrMsg: rpc error: code = Unavailable desc = unexpected HTTP status code received from server: 429 (Too Many Requests); transport: received unexpected content-type "application/json"`),
		},
	}
	for _, tc := range rateLimited {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, isRateLimitFailure(tc.err), "must be read as rate-limited: %v", tc.err)
		})
	}

	notRateLimited := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{
			// A genuine capability gap. Must still demote — this is the dwellir/STRK case.
			"upstream cannot serve the addon",
			errors.New("[-] verify failed to parse result {chainId:BERAB,Method:eth_getBlockByNumber,Response:{\"error\":{\"code\":-16503,\"message\":\"No healthy upstream available\"}}}"),
		},
		{
			"node error unrelated to rate limiting",
			errors.New("[-] verify failed sending chainMessage ErrMsg: txIndex 2: nonce too high"),
		},
		{
			// A block number that happens to contain 429 must not be mistaken for a status code.
			"block number containing 429",
			errors.New("[-] verify failed to parse result {block:4293102,Method:eth_getBlockByNumber}"),
		},
		{
			"connection refused",
			errors.New("dial tcp 1.2.3.4:443: connect: connection refused"),
		},
		{
			// A different status code must not ride the same path.
			"504 sentinel, wrapped the same way",
			utils.FormatWarning("Received invalid status code", common.StatusCodeError504),
		},
	}
	for _, tc := range notRateLimited {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, isRateLimitFailure(tc.err), "must NOT be read as rate-limited: %v", tc.err)
		})
	}
}
