package grpcproxy

import (
	"context"
	"strconv"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/testproto"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// A call whose handling stops with a panic, in the unary callback or in the stream callback, is
// answered INTERNAL with one fixed sentence and its request id in lava-guid, the id the callback
// saw. The server keeps serving: the next call on the same connection is answered.
func TestGRPCProxy_AHandlerPanicAnswersInternalAndKeepsServing(t *testing.T) {
	// failing decodes the request and reports the request id when the request asks to fail.
	failing := func(t *testing.T, ctx context.Context, reqBody []byte, ids chan<- uint64) bool {
		req := new(testproto.TestRequest)
		require.NoError(t, req.Unmarshal(reqBody))
		if req.Request != "fail" {
			return false
		}
		guid, found := utils.GetUniqueIdentifier(ctx)
		require.True(t, found, "the callback gets the request id")
		ids <- guid
		return true
	}
	for name, newProxy := range map[string]func(t *testing.T, ids chan<- uint64) (*grpc.Server, error){
		"in the unary callback": func(t *testing.T, ids chan<- uint64) (*grpc.Server, error) {
			srv, _, err := newGRPCProxy(func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error) {
				if failing(t, ctx, reqBody, ids) {
					panic("the handler failed")
				}
				return unaryEcho(t)(ctx, method, reqBody)
			}, "", testCmdFlags(), nil)
			return srv, err
		},
		"in the stream callback": func(t *testing.T, ids chan<- uint64) (*grpc.Server, error) {
			srv, _, err := NewGRPCProxyWithReflection(unaryEcho(t), "", testCmdFlags(), nil, nil,
				func(ctx context.Context, method string, reqBody []byte) (*StreamResponse, error) {
					if failing(t, ctx, reqBody, ids) {
						panic("the handler failed")
					}
					return nil, nil //nolint:nilnil // not a stream: the unary callback answers
				})
			return srv, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			ids := make(chan uint64, 1)
			srv, err := newProxy(t, ids)
			require.NoError(t, err)
			client := testproto.NewTestClient(testproto.InMemoryClientConn(t, srv))

			var trailer metadata.MD
			_, err = client.Test(context.Background(), &testproto.TestRequest{Request: "fail"}, grpc.Trailer(&trailer))
			require.Equal(t, codes.Internal, status.Code(err), "%v", err)
			require.Equal(t, HandlerFailedMessage, status.Convert(err).Message())
			require.Equal(t, []string{strconv.FormatUint(<-ids, 10)}, trailer.Get(common.GUID_HEADER_NAME), "the request id the callback saw")

			resp, err := client.Test(context.Background(), &testproto.TestRequest{Request: "echo"})
			require.NoError(t, err, "the server keeps serving")
			require.Equal(t, "echo-unary", resp.Response)
		})
	}
}
