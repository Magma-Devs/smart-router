package rpcsmartrouter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// MAG-2724, router side. The duplicate-name check belongs to the boot path, not to the parser:
// `smart-router health` is what an operator reaches for to work out why a config will not boot, so
// it has to be able to load the config the router rejects.

const mag2724DuplicateConfig = `direct-rpc:
  - name: SimTwin
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://one.example.com
  - name: SimTwin
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://two.example.com
  - name: solo
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://three.example.com
backup-direct-rpc:
  - name: SimTwin
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://backup.example.com
`

// TestParseStaticProviderEndpoints_AcceptsDuplicateNames states the split of responsibility:
// parsing is parsing. A name collision is a property of the whole set of lists a router loads —
// static and backup are looked up against each other by address, so only a check over both together
// is complete — and the boot path runs exactly that check. Rejecting inside the parser instead made
// every non-boot consumer of a config, health included, inherit the boot path's strictness.
func TestParseStaticProviderEndpoints_AcceptsDuplicateNames(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(mag2724DuplicateConfig)))

	endpoints, err := ParseStaticProviderEndpoints(v, common.DirectRPCConfigName)
	require.NoError(t, err, "the parser reports what the file says; the boot path decides whether to accept it")
	require.Len(t, endpoints, 3)

	// And the boot path is where it is rejected.
	require.Error(t, routersession.ValidateUniqueProviderNames(endpoints))
}

// TestCollectHealthProviders_LoadsConfigWithDuplicateNames pins that `smart-router health` still
// works on a config the router refuses to start on. That config is precisely the one an operator
// runs this command against — a diagnostic that declines to load the broken input is no diagnostic.
// Every configured node is probed, including both halves of the collision, so the report says which
// of the two is actually failing.
func TestCollectHealthProviders_LoadsConfigWithDuplicateNames(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dup.yml"), []byte(mag2724DuplicateConfig), 0o600))
	t.Chdir(dir)
	// collectHealthProviders drives the global viper; leave it as we found it.
	t.Cleanup(viper.Reset)

	providers, err := collectHealthProviders([]string{"dup"}, true)
	require.NoError(t, err, "health must be able to load the config it is reached for, not refuse it")
	require.Len(t, providers, 4, "both halves of the collision are probed, plus the unique provider and the backup")

	// The colliding rows carry the same name — the router's identity for them really is ambiguous,
	// and inventing a distinct label here would misrepresent what it would do with this config.
	// They are told apart by url, which is what identifies the node.
	urls := make([]string, 0, len(providers))
	for _, p := range providers {
		require.NotEmpty(t, p.nodeUrls)
		urls = append(urls, p.nodeUrls[0].Url)
	}
	require.ElementsMatch(t, []string{
		"https://one.example.com",
		"https://two.example.com",
		"https://three.example.com",
		"https://backup.example.com",
	}, urls)
}
