package statetracker

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// loadLockedSpec writes specJSON beside the bundled specs, loads the endpoint's spec through the boot
// path and returns the parser and the write-method lock's report.
func loadLockedSpec(t *testing.T, chainID, apiInterface, specJSON string) (chainlib.ChainParser, writeLockReport) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "locked.json"), []byte(specJSON), 0o600))
	chainParser, err := chainlib.NewChainParser(apiInterface)
	require.NoError(t, err)
	report, err := LoadStaticSpecs(context.Background(), chainParser, []string{"../../specs/", dir},
		routersession.RPCEndpoint{ChainID: chainID, ApiInterface: apiInterface}, "", "")
	require.NoError(t, err)
	return chainParser, report
}

type writeLockReport interface {
	Count(apiInterface string) int
}

// api is one api of a test spec's collection, with the given stateful and deterministic flags.
func api(name string, stateful int, deterministic bool) string {
	det := "false"
	if deterministic {
		det = "true"
	}
	return `{"name": "` + name + `", "enabled": true, "compute_units": 10,
		"block_parsing": {"parser_arg": ["latest"], "parser_func": "DEFAULT"},
		"category": {"deterministic": ` + det + `, "stateful": ` + itoa(stateful) + `, "hanging_api": true}}`
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	return "1"
}

// A spec that clears stateful on a write it declares still loads the write stateful, on every
// interface the lock speaks (PRD §5.3, §8.3; MAG-5593): the parser resolves the request to the forced
// api, so the relay fans it out and never sends it again, the cache stays out of it whatever
// `deterministic` says, and a batch holding it is stateful. The report counts it for the endpoint's
// own interface and reads 0 for the others, which is what the gauge publishes.
func TestStaticSpecLoadForcesAClearedWriteStateful(t *testing.T) {
	for _, tc := range []struct {
		name, chainID, apiInterface, specJSON string
		parse                                 func(t *testing.T, chainParser chainlib.ChainParser) chainlib.ChainMessage
	}{
		{
			name: "eth_sendRawTransaction, stateful 0 and deterministic", chainID: "EVMLOCK", apiInterface: spectypes.APIInterfaceJsonRPC,
			specJSON: `{"proposal": {"specs": [{"index": "EVMLOCK", "name": "evm lock", "enabled": true, "imports": ["ETH1"], "average_block_time": 2000,
				"api_collections": [{"enabled": true, "collection_data": {"api_interface": "jsonrpc", "internal_path": "", "type": "POST", "add_on": ""},
				"apis": [` + api("eth_sendRawTransaction", 0, true) + `]}]}]}}`,
			parse: func(t *testing.T, chainParser chainlib.ChainParser) chainlib.ChainMessage {
				msg, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x01"],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
				require.NoError(t, err)
				return msg
			},
		},
		{
			name: "a batch holding the forced write", chainID: "EVMLOCK", apiInterface: spectypes.APIInterfaceJsonRPC,
			specJSON: `{"proposal": {"specs": [{"index": "EVMLOCK", "name": "evm lock", "enabled": true, "imports": ["ETH1"], "average_block_time": 2000,
				"api_collections": [{"enabled": true, "collection_data": {"api_interface": "jsonrpc", "internal_path": "", "type": "POST", "add_on": ""},
				"apis": [` + api("eth_sendRawTransaction", 0, false) + `]}]}]}}`,
			parse: func(t *testing.T, chainParser chainlib.ChainParser) chainlib.ChainMessage {
				msg, err := chainParser.ParseMsg("", []byte(`[{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1},{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x01"],"id":2}]`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
				require.NoError(t, err)
				return msg
			},
		},
		{
			name: "a URI-form Tendermint broadcast_tx_sync", chainID: "TMLOCK", apiInterface: spectypes.APIInterfaceTendermintRPC,
			specJSON: `{"proposal": {"specs": [{"index": "TMLOCK", "name": "tendermint lock", "enabled": true, "imports": ["TENDERMINT"], "average_block_time": 6000,
				"api_collections": [{"enabled": true, "collection_data": {"api_interface": "tendermintrpc", "internal_path": "", "type": "", "add_on": ""},
				"apis": [` + api("broadcast_tx_sync", 0, false) + `]}]}]}}`,
			parse: func(t *testing.T, chainParser chainlib.ChainParser) chainlib.ChainMessage {
				msg, err := chainParser.ParseMsg("broadcast_tx_sync?tx=0x01", nil, "", nil, extensionslib.ExtensionInfo{})
				require.NoError(t, err)
				return msg
			},
		},
		{
			name: "Aptos POST /transactions/batch", chainID: "APTLOCK", apiInterface: spectypes.APIInterfaceRest,
			specJSON: `{"proposal": {"specs": [{"index": "APTLOCK", "name": "aptos lock", "enabled": true, "imports": ["APT1"], "average_block_time": 250,
				"api_collections": [{"enabled": true, "collection_data": {"api_interface": "rest", "internal_path": "", "type": "POST", "add_on": ""},
				"apis": [` + api("/transactions/batch", 0, false) + `]}]}]}}`,
			parse: func(t *testing.T, chainParser chainlib.ChainParser) chainlib.ChainMessage {
				msg, err := chainParser.ParseMsg("/transactions/batch", []byte(`[]`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
				require.NoError(t, err)
				return msg
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chainParser, report := loadLockedSpec(t, tc.chainID, tc.apiInterface, tc.specJSON)
			require.Equal(t, 1, report.Count(tc.apiInterface), "the gauge's value for this endpoint")
			for _, other := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC, spectypes.APIInterfaceRest, spectypes.APIInterfaceGrpc, spectypes.APIInterfaceGraphQL} {
				if other != tc.apiInterface {
					require.Equal(t, 0, report.Count(other), "0 elsewhere: %s", other)
				}
			}
			msg := tc.parse(t, chainParser)
			require.Equal(t, uint32(common.CONSISTENCY_SELECT_ALL_PROVIDERS), chainlib.GetStateful(msg), "the request resolves to the forced api")
		})
	}
}

// The gauge reads 0 for a chain whose spec marks its writes: the bundled ETH1 does.
func TestStaticSpecLoadReportsNothingForcedOnAWellMarkedSpec(t *testing.T) {
	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	report, err := LoadStaticSpecs(context.Background(), chainParser, []string{"../../specs/"},
		routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}, "", "")
	require.NoError(t, err)
	require.Equal(t, 0, report.Count(spectypes.APIInterfaceJsonRPC))
}
