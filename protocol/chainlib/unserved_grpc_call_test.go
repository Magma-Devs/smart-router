package chainlib

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
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
