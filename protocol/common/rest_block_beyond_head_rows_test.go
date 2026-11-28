package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-5760: the sidecar's and the gRPC gateway's "beyond my head" rows read the node's own sentence
// at the start of the message, where the node writes it. A node's 400 that quotes the request (the
// gateway's for a path parameter that does not parse) keeps the 400's verdict.
func TestRESTBlockBeyondHeadRows(t *testing.T) {
	const (
		sidecar = "Specified block number is larger than the current largest block. The largest known block number is 21144972."
		gateway = "requested block height is bigger then the chain length"
	)
	for _, tc := range []struct {
		name    string
		code    int
		message string
		want    *RouterError
	}{
		{"the sidecar's sentence", 400, sidecar, RouterErrorChainBlockNotFound},
		{"the gateway's sentence", 400, gateway, RouterErrorChainBlockNotFound},
		{"the sidecar's sentence, on the sender's second pass", 400, "HTTP 400: " + sidecar, RouterErrorChainBlockNotFound},
		{"the gateway's sentence, on the sender's second pass", 400, "HTTP 400: " + gateway, RouterErrorChainBlockNotFound},
		// A gateway that asks its gRPC service through an ABCI query (Cosmos SDK v0.45; a later node
		// with its gRPC server off): baseapp's gRPCErrorToSDKError wraps the service's status, and
		// the client's sdkErrorToGRPCError puts that log in the gateway's message.
		{"the gateway's sentence through an ABCI query", 400, "rpc error: code = InvalidArgument desc = " + gateway + ": invalid request", RouterErrorChainBlockNotFound},
		{"the gateway's sentence through an ABCI query, on the sender's second pass", 400, "HTTP 400: rpc error: code = InvalidArgument desc = " + gateway + ": invalid request", RouterErrorChainBlockNotFound},
		{
			"a path parameter that quotes the gateway's sentence with the status lead", 400,
			`type mismatch, parameter: height, error: strconv.ParseInt: parsing "rpc error: code = InvalidArgument desc = requested block height is bigger then the chain length": invalid syntax`,
			RouterErrorUserInvalidRequest,
		},
		{
			"a path parameter that quotes the gateway's sentence", 400,
			`type mismatch, parameter: height, error: strconv.ParseInt: parsing "requested block height is bigger then the chain length": invalid syntax`,
			RouterErrorUserInvalidRequest,
		},
		{
			"a path parameter that quotes the sidecar's sentence", 400,
			`Cannot get block hash for "larger than the current largest block"`,
			RouterErrorUserInvalidRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, family := range []ChainFamily{ChainFamilySubstrate, ChainFamilyCosmosSDK, ChainFamilyEVM, ChainFamilyUnknown} {
				require.Equal(t, tc.want, ClassifyError(nil, family, TransportREST, tc.code, tc.message), "family %d", family)
			}
		})
	}
}
