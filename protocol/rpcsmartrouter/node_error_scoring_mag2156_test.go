package rpcsmartrouter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestShouldFailSessionForResult_MAG2156 pins the gate that decides whether a completed direct-RPC
// relay reaches the optimizer as an availability sample of 0 (OnSessionFailure) or 1
// (OnSessionDone).
//
// The regression: a JSON-RPC node error is HTTP 200 with {"error":...} in the body, so the old
// `err != nil || statusCode >= 500 || statusCode == 429` test never fired for it and a failed
// response was scored as a success. The "node error over HTTP 200" case below is the one that
// failed before the fix.
func TestShouldFailSessionForResult_MAG2156(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		result *common.RelayResult
		want   bool
		why    string
	}{
		{
			name:   "transport error",
			err:    errors.New("connection refused"),
			result: &common.RelayResult{StatusCode: 0},
			want:   true,
			why:    "a transport failure has always been a session failure",
		},
		{
			name:   "clean success",
			result: &common.RelayResult{StatusCode: 200},
			want:   false,
			why:    "a healthy 200 must score as a success",
		},
		{
			name:   "node error over HTTP 200",
			result: &common.RelayResult{StatusCode: 200, IsNodeError: true},
			want:   true,
			why:    "MAG-2156: the JSON-RPC error envelope the optimizer used to score as a success",
		},
		{
			name:   "non-retryable node error over HTTP 200",
			result: &common.RelayResult{StatusCode: 200, IsNodeError: true, IsNonRetryable: true},
			want:   false,
			why:    "deterministic caller-fault errors come back from every provider; scoring them would demote the whole pairing",
		},
		{
			name:   "unsupported method",
			result: &common.RelayResult{StatusCode: 200, IsNodeError: true, IsNonRetryable: true, IsUnsupportedMethod: true},
			want:   false,
			// The gate never reads IsUnsupportedMethod — this passes on IsNonRetryable alone. That
			// is the point: all four SubCategoryUnsupportedMethod codes (2001/2008/2009/2010) are
			// Retryable=false, so the "no provider scoring" contract is upheld by the retryability
			// carve-out without the gate needing a third condition. If a future unsupported-method
			// code is ever registered Retryable=true, this case still passes but the contract
			// breaks — see TestUnsupportedMethodCodesAreAllNonRetryable_MAG2156 below, which is
			// what actually guards it.
			why: "SubCategoryUnsupportedMethod is contractually 'no provider scoring', delivered here via Retryable=false",
		},
		{
			name:   "retryable rate limit",
			result: &common.RelayResult{StatusCode: 200, IsNodeError: true, IsRateLimited: true},
			want:   false,
			why:    "NODE_RATE_LIMITED (2005) is Retryable=true, so only the rate-limit axis can carve it out: 'healthy but busy, do not mark unhealthy'",
		},
		{
			name:   "non-retryable rate limit",
			result: &common.RelayResult{StatusCode: 200, IsNodeError: true, IsNonRetryable: true, IsRateLimited: true},
			want:   false,
			why:    "NODE_LIMIT_EXCEEDED (2011) shares the subcategory with opposite retryability and must reach the same verdict",
		},
		{
			name:   "unclassified node error",
			result: &common.RelayResult{StatusCode: 200, IsNodeError: true},
			want:   true,
			why:    "KNOWN AND ACCEPTED: an unmatched error body has every flag false, so it scores. An endpoint failing in a way we have not catalogued is still failing — see shouldFailSessionForResult's doc comment",
		},
		{
			name:   "upstream 500",
			result: &common.RelayResult{StatusCode: 500},
			want:   true,
			why:    "REST 5xx fails the session with err == nil",
		},
		{
			name:   "upstream 429",
			result: &common.RelayResult{StatusCode: 429},
			want:   true,
			why:    "rate limit fails the session (pre-existing behaviour, preserved)",
		},
		{
			name:   "client error 400",
			result: &common.RelayResult{StatusCode: 400},
			want:   false,
			why:    "a 4xx other than 429 is the caller's fault, not the node's",
		},
		{
			name:   "node error on a 5xx",
			result: &common.RelayResult{StatusCode: 503, IsNodeError: true},
			want:   true,
			why:    "status-code arm still fires when both signals are present",
		},
		{
			name:   "nil result without error",
			result: nil,
			want:   false,
			why:    "must not panic, and must not invent a failure it has no evidence for",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldFailSessionForResult(tt.err, tt.result), tt.why)
		})
	}
}
