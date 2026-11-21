package rpcsmartrouter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// unlistedServerStatuses are REST 5xx statuses no status row names.
var unlistedServerStatuses = []int{505, 506, 507, 508, 510, 511, 599}

// TestRESTRelay_UnlistedServerError_IsAProviderFault_MAG5490: a bare REST 5xx that no status row
// names, empty or a proxy's HTML page, is a provider fault (rulebook Part V 3.1 P8): retried on
// another provider, and the endpoint at fault. It was UNKNOWN_ERROR: retried, but recorded and
// counted as nobody's fault.
func TestRESTRelay_UnlistedServerError_IsAProviderFault_MAG5490(t *testing.T) {
	for _, status := range unlistedServerStatuses {
		for _, body := range []string{"", fmt.Sprintf(`<html><body><h1>%d</h1><hr><center>nginx</center></body></html>`, status)} {
			t.Run(fmt.Sprintf("%d/%q", status, body), func(t *testing.T) {
				result := sendRESTThroughMockUpstream(t, status, body)
				assert.True(t, result.IsNodeError)
				assert.False(t, result.IsNonRetryable, "retried on another provider")
				assert.True(t, result.IsNodeAtFault, "a 5xx no row names is the endpoint's fault")
			})
		}
	}
}

// Through relayInnerDirect such a reply is still a transport error, JSON body or not, as a 502 or 504
// is: the request may have reached the node, so a write answered with one stays unclear. Its error
// is classified as the provider fault, and still may have reached the node.
func TestRESTRelay_UnlistedServerError_StaysATransportError_MAG5490(t *testing.T) {
	for _, status := range unlistedServerStatuses {
		for _, body := range []string{"", `{"message":"upstream error"}`} {
			t.Run(fmt.Sprintf("%d/%q", status, body), func(t *testing.T) {
				relayResult, relayErr := relayInnerDirectREST(t, status, body)
				require.Error(t, relayErr, "still a transport error")
				assert.True(t, relayResult.IsNodeAtFault)

				classified, _ := classifyDirectRPCError(relayErr, common.ChainFamilyUnknown, common.TransportREST)
				assert.Same(t, common.RouterErrorNodeUnlistedServerError, classified)
				assert.True(t, classified.MayHaveReachedNode, "a write answered with it stays unclear")
			})
		}
	}
}
