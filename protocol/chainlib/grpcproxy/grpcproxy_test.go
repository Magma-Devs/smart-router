package grpcproxy

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/testproto"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestGRPCProxy(t *testing.T) {
	proxyGRPCSrv, _, err := NewGRPCProxy(func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error) {
		// the callback function just does echo proxying
		req := new(testproto.TestRequest)
		err := req.Unmarshal(reqBody)
		require.NoError(t, err)
		respBytes, err := (&testproto.TestResponse{Response: req.Request + "-callback"}).Marshal()
		require.NoError(t, err)
		responseHeaders := make(metadata.MD)
		responseHeaders["test-headers"] = append(responseHeaders["test-headers"], "55")
		return respBytes, responseHeaders, nil
	}, "", common.ConsumerCmdFlags{HeadersFlag: "*", OriginFlag: "*", MethodsFlag: "GET,POST,OPTIONS", CDNCacheDuration: "86400"}, nil)
	require.NoError(t, err)

	client := testproto.NewTestClient(testproto.InMemoryClientConn(t, proxyGRPCSrv))
	ctx := context.Background()

	do := func() {
		req := &testproto.TestRequest{Request: "echo"}
		resp, err := client.Test(ctx, req)
		require.NoError(t, err)
		require.Equal(t, req.Request+"-callback", resp.Response)
	}

	do()
	do()
}

// TestGRPCProxy_MetadataOnError proves the proxy surfaces callback response metadata as gRPC trailers on
// the error path — the channel that carries lava-cross-validation-* failure headers to gRPC clients when a
// relay fails. Before the fix the handler returned the error before setting any metadata, so it was dropped.
func TestGRPCProxy_MetadataOnError(t *testing.T) {
	t.Run("error with metadata is surfaced as trailers", func(t *testing.T) {
		proxyGRPCSrv, _, err := NewGRPCProxy(func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error) {
			md := metadata.MD{
				common.CROSS_VALIDATION_STATUS_HEADER_NAME:    []string{"failed"},
				common.CROSS_VALIDATION_FAILURE_REASON_HEADER: []string{common.CrossValidationReasonNoAgreement},
			}
			return nil, md, errors.New("quorum failed")
		}, "", common.ConsumerCmdFlags{HeadersFlag: "*", OriginFlag: "*", MethodsFlag: "GET,POST,OPTIONS", CDNCacheDuration: "86400"}, nil)
		require.NoError(t, err)

		client := testproto.NewTestClient(testproto.InMemoryClientConn(t, proxyGRPCSrv))
		var trailer metadata.MD
		_, callErr := client.Test(context.Background(), &testproto.TestRequest{Request: "echo"}, grpc.Trailer(&trailer))
		require.Error(t, callErr, "the relay error must still propagate to the client")
		// Keys are lowercased by the proxy; the CV header consts are already lowercase.
		require.Equal(t, []string{"failed"}, trailer.Get(common.CROSS_VALIDATION_STATUS_HEADER_NAME))
		require.Equal(t, []string{common.CrossValidationReasonNoAgreement}, trailer.Get(common.CROSS_VALIDATION_FAILURE_REASON_HEADER))
	})

	t.Run("error without metadata leaves trailers empty (non-CV errors unchanged)", func(t *testing.T) {
		proxyGRPCSrv, err := func() (*grpc.Server, error) {
			s, _, e := NewGRPCProxy(func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error) {
				return nil, nil, errors.New("plain failure")
			}, "", common.ConsumerCmdFlags{HeadersFlag: "*", OriginFlag: "*", MethodsFlag: "GET,POST,OPTIONS", CDNCacheDuration: "86400"}, nil)
			return s, e
		}()
		require.NoError(t, err)

		client := testproto.NewTestClient(testproto.InMemoryClientConn(t, proxyGRPCSrv))
		var trailer metadata.MD
		_, callErr := client.Test(context.Background(), &testproto.TestRequest{Request: "echo"}, grpc.Trailer(&trailer))
		require.Error(t, callErr)
		require.Empty(t, trailer.Get(common.CROSS_VALIDATION_FAILURE_REASON_HEADER))
	})
}
