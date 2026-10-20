package chainlib

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

func TestUnsendableGrpcHeader(t *testing.T) {
	for _, tc := range []struct {
		value string
		bad   bool
	}{
		{"1000", false},
		{"a b~", false},
		{"10\xff", true},
		{"10\n", true},
		{"é", true},
	} {
		name, bad := unsendableGrpcHeader([]pairingtypes.Metadata{{Name: "x-cosmos-block-height", Value: tc.value}})
		require.Equal(t, tc.bad, bad, "%q", tc.value)
		if bad {
			require.Equal(t, "x-cosmos-block-height", name)
		}
	}
	_, bad := unsendableGrpcHeader([]pairingtypes.Metadata{{Name: "trace-bin", Value: "\x00\xff"}})
	require.False(t, bad, "a binary header carries any bytes")
}

// A forwarded header gRPC cannot send is refused while the request is parsed, before any
// endpoint is chosen, so the refusal never counts against a node.
func TestGrpcParseMsgRefusesAHeaderGRPCCannotSend(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, _, err := CreateChainLibMocks(ctx, "GRPCTEST", spectypes.APIInterfaceGrpc,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), nil, "../../", nil)
	require.NoError(t, err)
	defer func() {
		if closeServer != nil {
			closeServer()
		}
	}()
	parsing, apiCollection, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	header, _, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)
	require.True(t, ok)
	parse := func(value string) error {
		_, err := chainParser.ParseMsg(parsing.ApiName, []byte{}, apiCollection.CollectionData.Type,
			[]pairingtypes.Metadata{{Name: header.GetApiName(), Value: value}}, extensionslib.ExtensionInfo{})
		return err
	}
	require.NoError(t, parse("244590"))
	require.Error(t, parse("24459\xff"))
}

func TestReplyMetadataValuesAreSendable(t *testing.T) {
	md := convertRelayMetaDataToMDMetaData([]pairingtypes.Metadata{
		{Name: "lava-user-request-type", Value: "Default-cosmos.Foo/\xff 50%"},
		{Name: "lava-identified-node-error", Value: "true"},
		{Name: "trace-bin", Value: "\xff"},
	})
	require.Equal(t, []string{"Default-cosmos.Foo/%FF 50%25"}, md["lava-user-request-type"])
	require.Equal(t, []string{"true"}, md["lava-identified-node-error"], "a sendable value is unchanged")
	require.Equal(t, []string{"\xff"}, md["trace-bin"], "binary headers are left alone")
	_, bad := unsendableGrpcHeader([]pairingtypes.Metadata{{Name: "lava-user-request-type", Value: md["lava-user-request-type"][0]}})
	require.False(t, bad)
}
