package chainlib

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
)

// keyEchoingBlockService refuses every GetLatestBlock as a gateway that names the key it was given
// does: PermissionDenied, with the key in the message.
type keyEchoingBlockService struct{ key string }

func (s keyEchoingBlockService) GetLatestBlock(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
	return nil, status.Error(codes.PermissionDenied, "invalid api key "+s.key)
}

// MAG-5698: the head read returns the node's error through a debug-level log line, which hands an
// error back as it was built when debug is off, and `smartrouter health` prints what it returns.
// At info, the router's default, the head read's error and the health row it becomes name a
// registered value as [redacted].
func TestGrpcHeadRead_AtInfoTheNodesMessageNamesTheKeyAsRedacted(t *testing.T) {
	const key = "MAG5698HeadReadKey0123456789"
	utils.RegisterSecrets(key)
	// At debug, FormatDebugErr returns its own text, redacted on its way out: not the path under
	// test. UseMockLogLevel's debug is put back after.
	utils.SetGlobalLoggingLevel("info")
	t.Cleanup(func() { utils.SetGlobalLoggingLevel("debug") })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "GRPCTEST")
	require.NoError(t, err)
	chainParser, err := NewChainParser(spectypes.APIInterfaceGrpc)
	require.NoError(t, err)
	chainParser.SetSpec(spec)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	srv.RegisterService(&mockGRPCServiceDesc, keyEchoingBlockService{key: key})
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	endpoint := &routersession.RPCProviderEndpoint{
		ChainID:      "GRPCTEST",
		ApiInterface: spectypes.APIInterfaceGrpc,
		NodeUrls:     []common.NodeUrl{{Url: lis.Addr().String()}},
	}
	chainRouter, err := GetChainRouter(ctx, 1, endpoint, chainParser)
	require.NoError(t, err)
	chainFetcher := NewChainFetcher(ctx, &ChainFetcherOptions{ChainRouter: chainRouter, ChainParser: chainParser, Endpoint: endpoint})

	t.Run("the head read's error", func(t *testing.T) {
		_, err := chainFetcher.FetchLatestBlockNum(ctx)
		require.Error(t, err)
		require.NotContains(t, err.Error(), key)
		require.Contains(t, err.Error(), "gRPC status PermissionDenied: invalid api key [redacted]")
	})
	t.Run("the health row", func(t *testing.T) {
		rows, _ := chainFetcher.ValidateReport(ctx)
		require.Len(t, rows, 1)
		require.NotContains(t, rows[0].LatestBlockError, key)
		require.Contains(t, rows[0].LatestBlockError, "invalid api key [redacted]")
	})
}

// MAG-5698: every text in a health row is redacted, as a log line is. An error a debug-level log
// line returned when debug was off is the error as it was built: a transport error names the url
// it was sent to, and a node's error names what the node said.
func TestReport_EveryTextIsRedacted(t *testing.T) {
	const key = "MAG5698ReportKey0123456789"
	utils.RegisterSecrets(key)
	url := common.NodeUrl{Url: "https://node.example.com/v2/" + key}
	outcome := nodeUrlOutcome{
		url:     url,
		served:  url,
		err:     errors.New(`Post "https://node.example.com/v2/` + key + `": connection refused`),
		headErr: errors.New("gRPC status PermissionDenied: invalid api key " + key),
		checks: []checkOutcome{{
			verification: VerificationContainer{Name: "chain-id"},
			err:          errors.New("latest block unavailable: invalid api key " + key),
		}},
	}
	admission, _ := admissionFrom([]nodeUrlOutcome{outcome})

	result := outcome.report(0, admission)
	require.Equal(t, `Post "https://node.example.com/[redacted]": connection refused`, result.Error)
	require.Equal(t, "gRPC status PermissionDenied: invalid api key [redacted]", result.LatestBlockError)
	require.Len(t, result.Verifications, 1)
	require.Equal(t, "latest block unavailable: invalid api key [redacted]", result.Verifications[0].Error)
	for name, text := range map[string]string{"URL": result.URL, "Refusal": result.Refusal, "Throttled": result.Throttled} {
		require.NotContains(t, text, key, name)
	}
}

// notAnAnswerMessage is a request on an interface that answers its errors inside a 2xx (a GraphQL
// request), whose reply reads as an error with the message given.
type notAnAnswerMessage struct {
	ChainMessageForSend
	message string
}

func (m notAnAnswerMessage) GetRPCMessage() rpcInterfaceMessages.GenericMessage {
	return &rpcInterfaceMessages.GraphQLMessage{}
}

func (m notAnAnswerMessage) CheckResponseError([]byte, int) (bool, string) {
	return true, m.message
}

// MAG-5698: NotAnAnswerReplyError's message is the node's own text, on the same way out as
// grpcNodeErrorReply's.
func TestNotAnAnswerReplyError_NamesARegisteredValueAsRedacted(t *testing.T) {
	const key = "MAG5698NotAnAnswerKey0123456789"
	utils.RegisterSecrets(key)
	err := NotAnAnswerReplyError(notAnAnswerMessage{message: "invalid api key " + key}, []byte(`{"errors":[{"message":"invalid api key"}]}`), http.StatusOK)
	require.EqualError(t, err, "reply is not an answer: invalid api key [redacted]")
}
