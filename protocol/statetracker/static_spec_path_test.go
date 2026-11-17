package statetracker

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// TestRegisterForSpecUpdatesOrSetStaticSpecsWithToken_MisconfiguredPathFailsFast is MAG-1881
// item 5.5.2: --use-static-spec's existing tests (cross_validation_boot_test.go,
// cross_validation_fleet_test.go, health_cmd_test.go) all fixture a VALID path; none proves a
// misconfigured one fails fast rather than hanging or panicking. A nonexistent local path never
// reaches the network (specfetcher.IsRemoteRepoURL is false for it), so os.Stat alone decides
// this — and chainParser is only touched on the success path, so nil is safe here.
func TestRegisterForSpecUpdatesOrSetStaticSpecsWithToken_MisconfiguredPathFailsFast(t *testing.T) {
	err := RegisterForSpecUpdatesOrSetStaticSpecsWithToken(
		context.Background(),
		nil,
		[]string{"/nonexistent/path/mag-1881-does-not-exist"},
		routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		"", "",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed loading specs from source")
}

func TestRegisterForSpecUpdatesOrSetStaticSpecsWithToken_NoPathsFailsFast(t *testing.T) {
	err := RegisterForSpecUpdatesOrSetStaticSpecsWithToken(
		context.Background(),
		nil,
		nil,
		routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		"", "",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no spec paths provided")
}
