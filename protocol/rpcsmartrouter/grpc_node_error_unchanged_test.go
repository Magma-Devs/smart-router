package rpcsmartrouter

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/emptypb"
)

// A node's gRPC answer reaches the client as the node sent it (MAG-5129): the status code, the
// message and the details, and the headers and trailers the chain spec passes, each in its own
// block. Driven end to end: a gRPC client calls the router's real listener, which relays through
// the router's real relay path to a gRPC node.

const (
	// nodeListedHeader and nodeListedTrailer are the names the test spec passes on the reply.
	nodeListedHeader  = "x-node-height"
	nodeListedTrailer = "x-node-checkpoint"
	// nodeUnlistedName is a name the spec does not pass, sent in both blocks.
	nodeUnlistedName = "x-node-internal"
)

var nodeReplyDirectives = []*spectypes.Header{
	{Name: nodeListedHeader, Kind: spectypes.Header_pass_reply},
	{Name: nodeListedTrailer, Kind: spectypes.Header_pass_reply},
}

// startAnsweringGRPCNode answers every call with answer (an empty message when answer is OK),
// sending header as the reply's header metadata and trailer as its trailer metadata. Reflection
// is refused, so a background descriptor lookup learns nothing.
func startAnsweringGRPCNode(t *testing.T, answer *status.Status, header, trailer metadata.MD) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		if method, _ := grpc.MethodFromServerStream(stream); strings.Contains(method, "ServerReflection") {
			return status.Error(codes.Unimplemented, "no reflection")
		}
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		if err := stream.SetHeader(header); err != nil {
			return err
		}
		stream.SetTrailer(trailer)
		if answer.Code() != codes.OK {
			return answer.Err()
		}
		return stream.SendMsg([]byte{})
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// callRouterOverGRPC starts a one-node router in front of the node at address, with the spec
// passing nodeReplyDirectives, and makes one call through its gRPC listener. It returns what
// the client received: the call's header and trailer metadata, and its error.
func callRouterOverGRPC(t *testing.T, address string) (header, trailer metadata.MD, callErr error) {
	t.Helper()
	server, _, _ := newSingleGRPCNodeRouter(t, address, nodeReplyDirectives...)
	endpoint := &routersession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: server.listenEndpoint.ChainID, ApiInterface: spectypes.APIInterfaceGrpc}
	conn := dialGRPCListener(t, endpoint, server, server.chainParser)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	callErr = conn.Invoke(ctx, "/"+grpcRateLimitTestMethod, &emptypb.Empty{}, &emptypb.Empty{}, grpc.Header(&header), grpc.Trailer(&trailer))
	return header, trailer, callErr
}

// suiStyleDetails are details of the kind a node attaches with tonic::Status::with_details: which
// field of the request is wrong, and the reason, which a client may decide on.
func suiStyleDetails(t *testing.T, description string) []protoadapt.MessageV1 {
	t.Helper()
	return []protoadapt.MessageV1{
		&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{
			{Field: "transaction.signatures", Description: description},
		}},
		&errdetails.ErrorInfo{Reason: "INVALID_SIGNATURE", Domain: "node.example", Metadata: map[string]string{"field": "signatures"}},
	}
}

func nodeStatus(t *testing.T, code codes.Code, message string, details ...protoadapt.MessageV1) *status.Status {
	t.Helper()
	st := status.New(code, message)
	if len(details) == 0 {
		return st
	}
	withDetails, err := st.WithDetails(details...)
	require.NoError(t, err)
	return withDetails
}

func requireSameDetails(t *testing.T, want, got *status.Status) {
	t.Helper()
	wantDetails, gotDetails := want.Proto().GetDetails(), got.Proto().GetDetails()
	require.Len(t, gotDetails, len(wantDetails), "every detail the node sent reaches the client")
	for i := range wantDetails {
		require.True(t, proto.Equal(wantDetails[i], gotDetails[i]), "detail %d: want %v, got %v", i, wantDetails[i], gotDetails[i])
	}
}

func TestGRPCNodeErrorReachesTheClientUnchanged(t *testing.T) {
	answer := nodeStatus(t, codes.InvalidArgument, "invalid transaction: signature does not match", suiStyleDetails(t, "signature 0 does not verify")...)
	header := metadata.Pairs(nodeListedHeader, "123", nodeUnlistedName, "header-value")
	trailer := metadata.Pairs(nodeListedTrailer, "456", nodeUnlistedName, "trailer-value",
		// A node that names itself like the router does is never believed.
		common.PROVIDER_ADDRESS_HEADER_NAME, "spoofed-by-the-node")

	_, gotTrailer, callErr := callRouterOverGRPC(t, startAnsweringGRPCNode(t, answer, header, trailer))

	got := status.Convert(callErr)
	require.Equal(t, answer.Code(), got.Code())
	require.Equal(t, answer.Message(), got.Message())
	requireSameDetails(t, answer, got)
	// The reply is trailers-only, as the node's was: everything travels in the trailer block.
	require.Equal(t, []string{"123"}, gotTrailer.Get(nodeListedHeader), "a header the spec passes reaches the client")
	require.Equal(t, []string{"456"}, gotTrailer.Get(nodeListedTrailer), "a trailer the spec passes reaches the client (MAG-5129)")
	require.Empty(t, gotTrailer.Get(nodeUnlistedName), "a name the spec does not pass is not forwarded")
	require.NotContains(t, gotTrailer.Get(common.PROVIDER_ADDRESS_HEADER_NAME), "spoofed-by-the-node")
}

// The message goes back as the node wrote it: the router adds no text of its own to an empty one,
// cuts no long one, and changes no character of one in another script.
func TestGRPCNodeErrorMessageReachesTheClientAsWritten(t *testing.T) {
	cases := map[string]string{
		"empty":       "",
		"8000 chars":  strings.Repeat("abcdefgh", 1000),
		"non-English": "معاملة غير صالحة: 签名无效 — подпись недействительна",
	}
	for name, message := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, callErr := callRouterOverGRPC(t, startAnsweringGRPCNode(t, status.New(codes.InvalidArgument, message), nil, nil))
			got := status.Convert(callErr)
			require.Equal(t, codes.InvalidArgument, got.Code())
			require.Equal(t, message, got.Message())
			require.Empty(t, got.Proto().GetDetails(), "a node that sent no details gets none added")
		})
	}
}

// The operator's key is withheld wherever the node repeats it, in the message as in the details,
// and nothing else of the answer changes.
func TestGRPCNodeErrorMasksTheKeyInMessageAndDetails(t *testing.T) {
	const key = "mag5129-endtoend-key-58c1f0"
	utils.RegisterSecrets(key)
	answer := nodeStatus(t, codes.PermissionDenied, "api key "+key+" is not allowed",
		&errdetails.ErrorInfo{Reason: "API_KEY_INVALID", Domain: "node.example", Metadata: map[string]string{"api_key": key, "plan": "free"}},
		&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "auth", Description: "unknown key " + key}}},
	)

	_, _, callErr := callRouterOverGRPC(t, startAnsweringGRPCNode(t, answer, nil, nil))

	got := status.Convert(callErr)
	require.Equal(t, codes.PermissionDenied, got.Code())
	require.Equal(t, "api key "+utils.RedactedMark+" is not allowed", got.Message())
	details := got.Details()
	require.Len(t, details, 2)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok, "detail 0 is %T", details[0])
	require.Equal(t, "API_KEY_INVALID", info.GetReason())
	require.Equal(t, "node.example", info.GetDomain())
	require.Equal(t, map[string]string{"api_key": utils.RedactedMark, "plan": "free"}, info.GetMetadata())
	badRequest, ok := details[1].(*errdetails.BadRequest)
	require.True(t, ok, "detail 1 is %T", details[1])
	require.Equal(t, "auth", badRequest.GetFieldViolations()[0].GetField())
	require.Equal(t, "unknown key "+utils.RedactedMark, badRequest.GetFieldViolations()[0].GetDescription())
}

// On a success the node's headers come back as headers and its trailers as trailers, each block
// reduced to what the spec passes.
func TestGRPCNodeReplyTrailersReachTheClientAsTrailers(t *testing.T) {
	header := metadata.Pairs(nodeListedHeader, "123", nodeUnlistedName, "header-value")
	trailer := metadata.Pairs(nodeListedTrailer, "456", nodeUnlistedName, "trailer-value")

	gotHeader, gotTrailer, callErr := callRouterOverGRPC(t, startAnsweringGRPCNode(t, status.New(codes.OK, ""), header, trailer))

	require.NoError(t, callErr)
	require.Equal(t, []string{"123"}, gotHeader.Get(nodeListedHeader))
	require.Equal(t, []string{"456"}, gotTrailer.Get(nodeListedTrailer), "a trailer the spec passes reaches the client as a trailer (MAG-5129)")
	require.Empty(t, gotHeader.Get(nodeListedTrailer), "a trailer is not moved into the header block")
	require.Empty(t, gotHeader.Get(nodeUnlistedName))
	require.Empty(t, gotTrailer.Get(nodeUnlistedName))
}
