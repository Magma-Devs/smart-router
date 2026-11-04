package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The transport types a gRPC failure as rate-limited from its message (RateLimitFromGRPC); the
// registry classifies the same reply from the same message. They read one list, so for every code
// and every rate-limit text the two agree (MAG-4278 item 5). Before, the registry knew two of the
// four texts and checked the code first, so a reply could be a typed rate limit on the error and an
// at-fault node error in the classification.
func TestGRPCRateLimitTexts_TheRegistryAgreesWithTheTransport(t *testing.T) {
	for code := uint32(0); code <= 16; code++ {
		if _, kept := grpcCodesWhoseVerdictARateLimitTextDoesNotOverride[code]; kept {
			continue
		}
		for _, text := range GRPCRateLimitTexts {
			message := "upstream said: " + text
			_, typed := RateLimitFromGRPC(code, message, nil)
			classified := ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, int(code), message)
			assert.True(t, typed, "code %d %q", code, text)
			assert.Equal(t, LavaErrorNodeRateLimited, classified, "code %d %q", code, text)
		}
	}
}

// The two shapes the drift was found on.
func TestClassifyError_GRPCRateLimitShapesAreRateLimits(t *testing.T) {
	// A vendor's HTTP edge leaves its 429 inside codes.Unavailable: healthy but busy, hold off.
	// The code row alone made it NODE_SERVICE_UNAVAILABLE, which marks the endpoint unhealthy.
	unavailable := ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, 14, "unexpected HTTP status code received from server: 429 (Too Many Requests)")
	assert.Equal(t, LavaErrorNodeRateLimited, unavailable)
	assert.False(t, unavailable.EndpointAtFault())

	// RESOURCE_EXHAUSTED with the quota text had no row at all and scored as UNKNOWN_ERROR.
	exhausted := ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, 8, "too many requests, slow down")
	assert.Equal(t, LavaErrorNodeRateLimited, exhausted)

	// Without a rate-limit text the code rows still decide.
	assert.Equal(t, LavaErrorNodeServiceUnavailable, ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, 14, "connection draining"))
}

// The five registered codes whose own verdict still decides when the message also carries a
// rate-limit text. The transport types such a reply as rate-limited (RateLimitFromGRPC reads the
// text whatever the code), so here the two still disagree; closing that would make an
// INVALID_ARGUMENT or ABORTED retryable, which is a retry decision, not a vocabulary fix.
var grpcCodesWhoseVerdictARateLimitTextDoesNotOverride = map[uint32]*LavaError{
	3:  LavaErrorUserInvalidParams,
	5:  LavaErrorNodeDataNotHeld,
	10: LavaErrorNodeAborted,
	11: LavaErrorNodeDataNotHeld,
	12: LavaErrorNodeUnimplemented,
}

func TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide(t *testing.T) {
	for code, verdict := range grpcCodesWhoseVerdictARateLimitTextDoesNotOverride {
		for _, text := range GRPCRateLimitTexts {
			assert.Equal(t, verdict, ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, int(code), "upstream said: "+text), "code %d %q", code, text)
		}
	}
}
