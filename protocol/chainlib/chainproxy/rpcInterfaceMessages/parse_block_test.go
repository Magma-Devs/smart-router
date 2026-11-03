package rpcInterfaceMessages

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestParseBlockDelegatesToTheDefaultParser pins that every message type parses a block
// argument the same way: each ParseBlock is a one-line delegate to
// parser.ParseDefaultBlockParameter, which has its own tests, so these rows only need to
// show the delegation holds. The four per-interface copies this replaces were identical bar
// the type literal, and GraphQL, whose root-field input is what block parsing actually
// receives, had none.
func TestParseBlockDelegatesToTheDefaultParser(t *testing.T) {
	t.Parallel()

	graphQLMessage := &GraphQLMessage{}
	messages := map[string]interface{ ParseBlock(string) (int64, error) }{
		"grpc":              GrpcMessage{},
		"jsonrpc":           JsonrpcMessage{},
		"rest":              RestMessage{},
		"tendermintrpc":     TendermintrpcMessage{},
		"parsable rpc":      ParsableRPCInput{},
		"graphql":           graphQLMessage,
		"graphql rootfield": graphQLRootFieldInput{message: graphQLMessage},
	}

	cases := []struct {
		input    string
		expected int64
		wantErr  bool
	}{
		{input: "latest", expected: spectypes.LATEST_BLOCK},
		{input: "80", expected: 80},
		{input: "0x26D", expected: 621},
		{input: "validated", expected: spectypes.FINALIZED_BLOCK},
		{input: "not-a-block", expected: spectypes.NOT_APPLICABLE, wantErr: true},
	}

	for name, message := range messages {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range cases {
				block, err := message.ParseBlock(tc.input)
				if tc.wantErr {
					require.Error(t, err, tc.input)
				} else {
					require.NoError(t, err, tc.input)
				}
				require.Equal(t, tc.expected, block, tc.input)
			}
		})
	}
}
