package rpcsmartrouter

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// statusRuleBeforeTheRegistry is the hand table classifyHTTPStatus was before it read the registry:
// 5xx unhealthy and backed off, 429 backed off only, every other status neither.
func statusRuleBeforeTheRegistry(code int) (shouldMarkUnhealthy, needsBackoff bool) {
	switch {
	case code >= 500:
		return true, true
	case code == 429:
		return false, true
	default:
		return false, false
	}
}

// classifyHTTPStatus reads the registry's verdict for the status now, with the two disagreements
// held as overrides, so its answer is the hand table's for every status, on every chain family and
// on both tables a status can be classified on.
func TestClassifyHTTPStatus_GivesTheStatusRulesAnswerForEveryStatus(t *testing.T) {
	for family := common.ChainFamilyUnknown; family <= common.ChainFamilySui; family++ {
		for _, transport := range []common.TransportType{common.TransportREST, common.TransportJsonRPC} {
			for code := 100; code < 600; code++ {
				classified := common.ClassifyError(nil, family, transport, code, "")
				gotUnhealthy, gotBackoff := classifyHTTPStatus(code, classified)
				wantUnhealthy, wantBackoff := statusRuleBeforeTheRegistry(code)
				assert.Equal(t, wantUnhealthy, gotUnhealthy, "family %d transport %v status %d (%s)", family, transport, code, classified.Name)
				assert.Equal(t, wantBackoff, gotBackoff, "family %d transport %v status %d (%s)", family, transport, code, classified.Name)
			}
		}
	}
}

// The overrides are where the registry disagrees today; each is pinned so a decision on either
// is a visible change here (MAG-4278 item 4).
func TestClassifyHTTPStatus_OverridesWhereTheRegistryDisagrees(t *testing.T) {
	timeout := common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, http.StatusRequestTimeout, "")
	registryUnhealthy, _ := classifyEndpointHealth(timeout, false)
	assert.True(t, registryUnhealthy, "the registry blames a 408 (%s)", timeout.Name)
	unhealthy, backoff := classifyHTTPStatus(http.StatusRequestTimeout, timeout)
	assert.False(t, unhealthy)
	assert.False(t, backoff)

	unimplemented := common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, http.StatusNotImplemented, "")
	registryUnhealthy, _ = classifyEndpointHealth(unimplemented, false)
	assert.False(t, registryUnhealthy, "the registry reads a 501 as a capability answer (%s)", unimplemented.Name)
	unhealthy, backoff = classifyHTTPStatus(http.StatusNotImplemented, unimplemented)
	assert.True(t, unhealthy)
	assert.True(t, backoff)
}
