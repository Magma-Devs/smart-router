package rpcsmartrouter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

// A provider's exclusion reason is the refusal of its first failing url, built from that check's
// error as it came back: a transport error names the url it was sent to, and a node's error names
// what the node said. The health row prints it as `router.reason`, redacted like the url's own
// error and verification rows.
func TestRouterVerdict_TheExclusionReasonIsRedacted(t *testing.T) {
	const registered = "routerVerdictRegistered0123456789"
	utils.RegisterSecrets(registered)
	const unregistered = "routerVerdictUrlKey9876543210"
	cause := fmt.Sprintf(`latest block unavailable: Post "https://node.example.com/v2/%s": EOF; invalid api key %s`, unregistered, registered)

	excluded := routerVerdict(chainlib.NodeURLValidation{}, fmt.Errorf("no node url left serving the root path: pruning verification failed: %s", cause))
	require.Equal(t, routerExcluded, excluded.Verdict)
	require.Equal(t, `no node url left serving the root path: pruning verification failed: latest block unavailable: Post "https://node.example.com/[redacted]": EOF; invalid api key [redacted]`, excluded.Reason)

	throttled := routerVerdict(chainlib.NodeURLValidation{}, fmt.Errorf("no node url verified serving the root path: %s: %w", cause, commonlib.StatusCodeError429))
	require.Equal(t, routerUnknown, throttled.Verdict)
	for name, reason := range map[string]string{"excluded": excluded.Reason, "rate-limited": throttled.Reason} {
		require.NotContains(t, reason, unregistered, name)
		require.NotContains(t, reason, registered, name)
	}
}
