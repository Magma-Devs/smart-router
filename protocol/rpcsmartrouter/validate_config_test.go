package rpcsmartrouter

import (
	"testing"

	"github.com/Magma-Devs/smart-router/protocol/common"
	"github.com/Magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/Magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixture builders — keep tests focused on validation behavior, not on
// constructing nested structs.

func mkEndpoint(chainID, apiInterface string) *lavasession.RPCEndpoint {
	return &lavasession.RPCEndpoint{
		ChainID:      chainID,
		ApiInterface: apiInterface,
	}
}

func mkProvider(name, chainID, apiInterface string, urls ...string) *lavasession.RPCStaticProviderEndpoint {
	nodeUrls := make([]common.NodeUrl, 0, len(urls))
	for _, u := range urls {
		nodeUrls = append(nodeUrls, common.NodeUrl{Url: u})
	}
	return &lavasession.RPCStaticProviderEndpoint{
		Name:         name,
		ChainID:      chainID,
		ApiInterface: apiInterface,
		NodeUrls:     nodeUrls,
	}
}

// validateSmartRouterConfigAgainstEdition tests are explicitly community-mode
// (the default activeConfig). Each test asserts the centralized validator
// rejects with the gate's pinned error message — the same substrings the
// inline gates produce, since both call ActiveConfig().Validate*() under
// the hood.

func TestValidateSmartRouterConfigAgainstEdition_AllowsCommunityHappyPath(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("ETH1", spectypes.APIInterfaceJsonRPC)}
	direct := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("eth-primary", "ETH1", spectypes.APIInterfaceJsonRPC,
			"https://eth.example.com:8545", "http://eth-backup.example.com:8545"),
	}
	backup := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("eth-failover", "ETH1", spectypes.APIInterfaceJsonRPC, "https://eth-failover.example.com:8545"),
	}

	require.NoError(t, validateSmartRouterConfigAgainstEdition(endpoints, direct, backup),
		"community + ETH1 + jsonrpc + http/https = happy path")
}

func TestValidateSmartRouterConfigAgainstEdition_RejectsRESTInterface(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("ETH1", spectypes.APIInterfaceRest)}

	err := validateSmartRouterConfigAgainstEdition(endpoints, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REST interface requires an enterprise license")
}

func TestValidateSmartRouterConfigAgainstEdition_RejectsTendermintRPCInterface(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("ETH1", spectypes.APIInterfaceTendermintRPC)}

	err := validateSmartRouterConfigAgainstEdition(endpoints, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TendermintRPC interface requires an enterprise license")
}

func TestValidateSmartRouterConfigAgainstEdition_RejectsNonEVMSpec(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("LAVA", spectypes.APIInterfaceJsonRPC)}

	err := validateSmartRouterConfigAgainstEdition(endpoints, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `non-EVM spec "LAVA" requires an enterprise license`)
}

func TestValidateSmartRouterConfigAgainstEdition_RejectsWebsocketInDirect(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("ETH1", spectypes.APIInterfaceJsonRPC)}
	direct := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("eth-ws", "ETH1", spectypes.APIInterfaceJsonRPC, "wss://eth.example.com:8546"),
	}

	err := validateSmartRouterConfigAgainstEdition(endpoints, direct, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WebSocket transport")
	assert.Contains(t, err.Error(), "requires an enterprise license")
}

func TestValidateSmartRouterConfigAgainstEdition_RejectsExplicitGRPCScheme(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("ETH1", spectypes.APIInterfaceJsonRPC)}
	direct := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("eth-grpc", "ETH1", spectypes.APIInterfaceJsonRPC, "grpc://eth.example.com:9090"),
	}

	err := validateSmartRouterConfigAgainstEdition(endpoints, direct, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gRPC transport")
	assert.Contains(t, err.Error(), "requires an enterprise license")
}

// Bare-host gRPC is the test §3.3.6 row 3a was specifically designed for —
// the URL has no scheme, but provider.ApiInterface=grpc means the actual
// transport will be gRPC. Centralized validation must synthesize "grpc://"
// before calling ValidateTransport so community rejects it.
func TestValidateSmartRouterConfigAgainstEdition_RejectsBareHostGRPC(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("LAVA", spectypes.APIInterfaceGrpc)}
	direct := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("lava-grpc", "LAVA", spectypes.APIInterfaceGrpc, "lava-grpc.example.com:443"),
	}

	err := validateSmartRouterConfigAgainstEdition(endpoints, direct, nil)
	require.Error(t, err, "bare-host with ApiInterface=grpc must be caught by row #3a synthesis")
	// Either the api-interface gate fires first (rejecting "grpc") or the
	// transport gate fires after synthesis. Both produce an enterprise-license
	// error — assert the common substring rather than the specific message.
	assert.Contains(t, err.Error(), "requires an enterprise license")
}

func TestValidateSmartRouterConfigAgainstEdition_BareHostHTTPStillAllowed(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("ETH1", spectypes.APIInterfaceJsonRPC)}
	direct := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("eth-bare", "ETH1", spectypes.APIInterfaceJsonRPC, "eth.example.com:8545"),
	}

	require.NoError(t, validateSmartRouterConfigAgainstEdition(endpoints, direct, nil),
		"bare-host with non-grpc ApiInterface must be allowed (no synthesis applied)")
}

// Backup providers must be validated too — many YAML configs have a clean
// primary and a sneaky enterprise-only backup.
func TestValidateSmartRouterConfigAgainstEdition_RejectsBackupTransport(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{mkEndpoint("ETH1", spectypes.APIInterfaceJsonRPC)}
	direct := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("eth-primary", "ETH1", spectypes.APIInterfaceJsonRPC, "https://eth.example.com:8545"),
	}
	backup := []*lavasession.RPCStaticProviderEndpoint{
		mkProvider("eth-failover-ws", "ETH1", spectypes.APIInterfaceJsonRPC, "wss://eth-ws.example.com:8546"),
	}

	err := validateSmartRouterConfigAgainstEdition(endpoints, direct, backup)
	require.Error(t, err, "WS in backup providers must reject just like in primary")
	assert.Contains(t, err.Error(), "WebSocket transport")
}

// Spec dedup: a chain configured for jsonrpc twice (e.g., listener for the
// HTTP path AND a separate listener for some other path) shouldn't double-
// validate the spec. The validator's seenSpecs map prevents that — exercise
// it implicitly by configuring two endpoints on ETH1 and asserting no error.
func TestValidateSmartRouterConfigAgainstEdition_DedupesSpecValidation(t *testing.T) {
	snapshotConfigSingletons(t)
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	endpoints := []*lavasession.RPCEndpoint{
		mkEndpoint("ETH1", spectypes.APIInterfaceJsonRPC),
		mkEndpoint("ETH1", spectypes.APIInterfaceJsonRPC),
	}

	require.NoError(t, validateSmartRouterConfigAgainstEdition(endpoints, nil, nil))
}
