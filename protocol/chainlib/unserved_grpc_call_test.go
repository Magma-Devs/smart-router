package chainlib

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The router's refusal of a call type it does not serve is never retried: every provider would be
// asked the same call (MAG-5133).
func TestShouldRetryError_UnservedGRPCCallIsNeverRetried(t *testing.T) {
	err := fmt.Errorf("%w: gRPC method %q streams", common.UnservedGRPCCallError, "a.B/C")
	require.False(t, ShouldRetryError(err))
	require.False(t, ShouldRetryErrorWithContext(err, common.ChainFamily(-1), common.TransportGRPC))
}

type fixedCallShapes struct {
	clientStreams, serverStreams, known bool
}

func (f fixedCallShapes) GRPCCallShape(string) (bool, bool, bool) {
	return f.clientStreams, f.serverStreams, f.known
}

// fixedShapeSender is a relay sender that holds one definition for every method.
type fixedShapeSender struct {
	RelaySender
	fixedCallShapes
}

func callShapeListener(t *testing.T, shapes fixedCallShapes) *GrpcChainListener {
	t.Helper()
	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(`{
      "index": "GRPCTEST", "enabled": true, "average_block_time": 1000,
      "api_collections": [{
        "enabled": true,
        "collection_data": {"api_interface": "grpc", "internal_path": "", "type": "", "add_on": ""},
        "apis": [
          {"name": "a.Svc/Unary", "enabled": true, "compute_units": 10, "category": {}},
          {"name": "a.Svc/Off", "enabled": false, "compute_units": 10, "category": {}},
          {"name": "a.Svc/Subscribe", "enabled": true, "compute_units": 10, "category": {}}
        ],
        "parse_directives": [{"function_tag": "SUBSCRIBE", "api_name": "a.Svc/Subscribe"}]
      }]
    }`), &spec))
	parser, err := NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)
	return &GrpcChainListener{chainParser: parser, chainListenerBase: chainListenerBase{relaySender: fixedShapeSender{fixedCallShapes: shapes}}}
}

func TestUnservedCallReason(t *testing.T) {
	unary := fixedCallShapes{known: true}
	serverStream := fixedCallShapes{serverStreams: true, known: true}
	for _, tc := range []struct {
		name                string
		method              string
		shapes              fixedCallShapes
		servesSubscriptions bool
		refused             bool
	}{
		{"switched off", "a.Svc/Off", unary, true, true},
		{"single reply", "a.Svc/Unary", unary, true, false},
		{"no definition", "a.Svc/Other", fixedCallShapes{}, true, false},
		{"client stream", "a.Svc/Other", fixedCallShapes{clientStreams: true, known: true}, true, true},
		{"two-way stream", "a.Svc/Other", fixedCallShapes{clientStreams: true, serverStreams: true, known: true}, true, true},
		{"server stream the spec does not name", "a.Svc/Other", serverStream, true, true},
		{"subscription the spec names", "a.Svc/Subscribe", serverStream, true, false},
		{"subscription on a listener that serves none", "a.Svc/Subscribe", serverStream, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, message := callShapeListener(t, tc.shapes).unservedCallReason(tc.method, tc.servesSubscriptions)
			require.Equal(t, tc.refused, reason != "", "reason %q", reason)
			require.Equal(t, tc.refused, message != "")
		})
	}
}

// A refusal of the call type wins over the relay's no-answer verdict. The caller gets UNIMPLEMENTED
// and the router's sentence, never UNAVAILABLE, which a gRPC client may take as a cue to try the
// call again. The relay picks the error it returns and the verdict it hands the listener from its
// attempts separately, so the listener reads the refusal first instead of relying on that pick to
// keep the two apart.
func TestGrpcChainListener_UnservedRefusalWinsOverNoAnswer(t *testing.T) {
	sender := &stubRelaySender{
		parser:       grpcParserWithSubscription(),
		sendErr:      fmt.Errorf("failed relay, insufficient results ErrMsg: %w", common.UnservedGRPCCallError),
		failedResult: &common.RelayResult{ClientFacing: &common.ClientFacingError{RouterErrorCode: 1012, RouterErrorName: "PROTOCOL_PROVIDER_UNAVAILABLE"}},
	}
	conn, err := grpc.NewClient(startTestGrpcListener(t, sender), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reply []byte
	var trailer metadata.MD
	err = conn.Invoke(ctx, "/"+unaryApiName, []byte("{}"), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true), grpc.Trailer(&trailer))
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
	require.Equal(t, common.UnservedGRPCCallError.Error(), status.Convert(err).Message())
	require.NotEmpty(t, trailer.Get(common.GUID_HEADER_NAME), "the refusal carries the request id")
}
