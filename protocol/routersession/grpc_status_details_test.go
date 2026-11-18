package routersession

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const detailsTestKey = "mag5129-unit-key-9d2e41"

func init() { utils.RegisterSecrets(detailsTestKey) }

func mustAny(t *testing.T, m proto.Message) *anypb.Any {
	t.Helper()
	detail, err := anypb.New(m)
	require.NoError(t, err)
	return detail
}

// A detail that holds no secret comes back as the very value the node sent: not decoded and
// encoded again, so nothing about its bytes can change.
func TestRedactStatusDetails_CleanDetailsComeBackAsSent(t *testing.T) {
	info := mustAny(t, &errdetails.ErrorInfo{Reason: "ABOVE_LATEST_AVAILABLE", Domain: "node.example", Metadata: map[string]string{"latest": "100"}})
	retry := mustAny(t, &errdetails.RetryInfo{})
	got := redactStatusDetails([]*anypb.Any{info, retry})
	require.Len(t, got, 2)
	require.Same(t, info, got[0])
	require.Same(t, retry, got[1])
}

// The key is masked in every string field of a known detail, nested and repeated ones and map
// values included, and every other field keeps its value.
func TestRedactStatusDetails_MasksTheKeyInEveryStringField(t *testing.T) {
	badRequest := &errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{
		{Field: "auth", Description: "key " + detailsTestKey + " unknown"},
		{Field: "digest", Description: "missing"},
	}}
	info := &errdetails.ErrorInfo{Reason: "API_KEY_INVALID", Domain: "node.example", Metadata: map[string]string{"key": detailsTestKey, "plan": "free"}}
	sent := []*anypb.Any{mustAny(t, badRequest), mustAny(t, info)}

	got := redactStatusDetails(sent)

	require.Len(t, got, 2)
	require.Equal(t, sent[0].GetTypeUrl(), got[0].GetTypeUrl())
	var gotBadRequest errdetails.BadRequest
	require.NoError(t, got[0].UnmarshalTo(&gotBadRequest))
	require.Equal(t, "key "+utils.RedactedMark+" unknown", gotBadRequest.GetFieldViolations()[0].GetDescription())
	require.Equal(t, "auth", gotBadRequest.GetFieldViolations()[0].GetField())
	require.Equal(t, "missing", gotBadRequest.GetFieldViolations()[1].GetDescription())
	var gotInfo errdetails.ErrorInfo
	require.NoError(t, got[1].UnmarshalTo(&gotInfo))
	require.Equal(t, "API_KEY_INVALID", gotInfo.GetReason())
	require.Equal(t, map[string]string{"key": utils.RedactedMark, "plan": "free"}, gotInfo.GetMetadata())
}

// A detail nesting an Any is masked around it, and the nested Any is left whole: its type url
// reads as a url ("type.googleapis.com/google.rpc.ErrorInfo"), and masking would cut it.
func TestRedactStatusDetails_LeavesANestedAnyWhole(t *testing.T) {
	nested := mustAny(t, &errdetails.ErrorInfo{Reason: "INNER"})
	sent := mustAny(t, &spb.Status{Message: "outer " + detailsTestKey, Details: []*anypb.Any{nested}})

	got := redactStatusDetails([]*anypb.Any{sent})

	require.Len(t, got, 1)
	var outer spb.Status
	require.NoError(t, got[0].UnmarshalTo(&outer))
	require.Equal(t, "outer "+utils.RedactedMark, outer.GetMessage())
	require.Len(t, outer.GetDetails(), 1)
	require.True(t, proto.Equal(nested, outer.GetDetails()[0]))
}

// A detail of a type the router does not know is kept as sent when its bytes hold nothing the
// masking withholds, and left off when they do: its text cannot be told from the rest of it.
func TestRedactStatusDetails_UnknownTypeKeptOnlyWithoutASecret(t *testing.T) {
	clean := &anypb.Any{TypeUrl: "type.googleapis.com/vendor.v1.Opaque", Value: []byte{0x0a, 0x03, 'a', 'b', 'c'}}
	withKey := &anypb.Any{TypeUrl: "type.googleapis.com/vendor.v1.Opaque", Value: append([]byte{0x0a, byte(len(detailsTestKey))}, detailsTestKey...)}

	got := redactStatusDetails([]*anypb.Any{clean, withKey})

	require.Len(t, got, 1)
	require.Same(t, clean, got[0])
}

// A details-bearing reply survives the JSON it crosses the relay in, and the listener rebuilds
// the node's status from it whole. Details that do not read back cost the details only.
func TestGRPCNodeErrorResponse_StatusRebuildsTheNodesStatus(t *testing.T) {
	sent, err := status.New(codes.OutOfRange, "checkpoint 9 is above the latest").WithDetails(
		&errdetails.ErrorInfo{Reason: "AboveLatestAvailable", Domain: "node.example"})
	require.NoError(t, err)
	body, err := json.Marshal(GRPCNodeErrorResponse{
		ErrorMessage: sent.Message(),
		ErrorCode:    uint32(sent.Code()),
		ErrorDetails: marshalStatusDetails(sent.Proto().GetDetails()),
	})
	require.NoError(t, err)

	var carried GRPCNodeErrorResponse
	require.NoError(t, json.Unmarshal(body, &carried))
	require.True(t, proto.Equal(sent.Proto(), carried.Status().Proto()))

	carried.ErrorDetails = []byte{0xff, 0xff}
	rebuilt := carried.Status()
	require.Equal(t, codes.OutOfRange, rebuilt.Code())
	require.Equal(t, sent.Message(), rebuilt.Message())
	require.Empty(t, rebuilt.Proto().GetDetails())
}

// handleGRPCError carries the node's details, masked, in the reply body, and returns the node's
// trailers beside its headers, without the raw grpc-status-details-bin: an unmasked copy of the
// details must not travel next to the masked one, whatever a spec lists.
func TestHandleGRPCError_KeepsDetailsAndTrailers(t *testing.T) {
	sent, err := status.New(codes.InvalidArgument, "bad field").WithDetails(
		&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "auth", Description: detailsTestKey}}})
	require.NoError(t, err)
	header := metadata.Pairs("x-node-height", "1")
	trailer := metadata.Pairs("x-node-checkpoint", "2", grpcStatusDetailsTrailer, "raw-details-bytes")

	resp, callErr := (&GRPCDirectRPCConnection{}).handleGRPCError(context.Background(), sent.Err(), header, trailer, true)

	require.Error(t, callErr)
	require.NotNil(t, resp)
	require.Equal(t, []string{"1"}, resp.Metadata["x-node-height"])
	require.Equal(t, []string{"2"}, resp.Trailers["x-node-checkpoint"])
	require.NotContains(t, resp.Trailers, grpcStatusDetailsTrailer)
	require.Contains(t, trailer, grpcStatusDetailsTrailer, "the node's own metadata is not changed")

	var body GRPCNodeErrorResponse
	require.NoError(t, json.Unmarshal(resp.Data, &body))
	got := body.Status()
	require.Equal(t, codes.InvalidArgument, got.Code())
	require.Equal(t, "bad field", got.Message())
	details := got.Details()
	require.Len(t, details, 1)
	badRequest, ok := details[0].(*errdetails.BadRequest)
	require.True(t, ok, "detail is %T", details[0])
	require.Equal(t, "auth", badRequest.GetFieldViolations()[0].GetField())
	require.Equal(t, utils.RedactedMark, badRequest.GetFieldViolations()[0].GetDescription())
}
