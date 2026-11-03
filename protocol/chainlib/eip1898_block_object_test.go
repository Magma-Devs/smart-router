package chainlib

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// MAG-4758: against the real ETH1 spec, every state read that accepts an EIP-1898 block object
// treats {"blockNumber": X} exactly like X: the same requested block and, for an old block, the
// same archive routing at attempt 0. {"blockHash": …} names no height and stays NOT_APPLICABLE
// with no archive (MAG-4142).
func TestParseMsg_EIP1898BlockNumberObjectRoutesLikeTheHexForm(t *testing.T) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "ETH1")
	require.NoError(t, err)
	chainParser, err := NewJrpcChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(spec)
	chainParser.SetPolicyFromAddonAndExtensionMap(map[string]struct{}{extensionslib.ArchiveExtension: {}})

	// Far behind the tip, past every archive threshold in the ETH1 spec.
	const tip = 20_000_000
	const oldBlock = 0x10
	const address = `"0x1111111111111111111111111111111111111111"`

	// Each method's params with the block left as %s, at the index the spec's PARSE_BY_ARG reads.
	methods := map[string]string{
		"eth_call":                `[{"to":` + address + `,"data":"0x"},%s]`,
		"eth_getBalance":          `[` + address + `,%s]`,
		"eth_getCode":             `[` + address + `,%s]`,
		"eth_getTransactionCount": `[` + address + `,%s]`,
		"eth_getStorageAt":        `[` + address + `,"0x0",%s]`,
		"eth_getProof":            `[` + address + `,["0x0"],%s]`,
	}
	parse := func(method, params, block string) ChainMessage {
		t.Helper()
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, fmt.Sprintf(params, block))
		chainMessage, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: tip})
		require.NoError(t, err)
		return chainMessage
	}
	extensionNames := func(chainMessage ChainMessage) []string {
		names := []string{}
		for _, extension := range chainMessage.GetExtensions() {
			names = append(names, extension.Name)
		}
		return names
	}

	for method, params := range methods {
		t.Run(method, func(t *testing.T) {
			hexForm := parse(method, params, fmt.Sprintf(`"0x%x"`, oldBlock))
			hexBlock, _ := hexForm.RequestedBlock()
			require.Equal(t, int64(oldBlock), hexBlock, "control: the hex form parses to its height")
			require.Equal(t, []string{extensionslib.ArchiveExtension}, extensionNames(hexForm), "control: an old hex block goes to archive")

			objectForm := parse(method, params, fmt.Sprintf(`{"blockNumber":"0x%x"}`, oldBlock))
			objectBlock, _ := objectForm.RequestedBlock()
			require.Equal(t, hexBlock, objectBlock, "the blockNumber object names the same height as the hex form")
			require.Equal(t, extensionNames(hexForm), extensionNames(objectForm), "and routes the same way")

			hashForm := parse(method, params, `{"blockHash":"0x1111111111111111111111111111111111111111111111111111111111111111"}`)
			hashBlock, _ := hashForm.RequestedBlock()
			require.Equal(t, spectypes.NOT_APPLICABLE, hashBlock, "a blockHash object names no height (MAG-4142)")
			require.Empty(t, extensionNames(hashForm))
		})
	}
}
