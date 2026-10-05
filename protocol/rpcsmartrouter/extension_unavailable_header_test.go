package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3935: a request that asked for an extension no node offers is served without it, and the
// reply has to say so — otherwise a non-archive answer to an archive request reads as a real one.

func headerValues(metadata []pairingtypes.Metadata, name string) []string {
	var values []string
	for _, md := range metadata {
		if md.Name == name {
			values = append(values, md.Value)
		}
	}
	return values
}

func appendHeadersFor(t *testing.T, srv *RPCSmartRouterServer, unavailable []string) []pairingtypes.Metadata {
	t.Helper()
	relayResult := &common.RelayResult{
		ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
		Reply:        &pairingtypes.RelayReply{},
	}
	srv.appendHeadersToRelayResult(context.Background(), relayResult, 0, &MockRelayProcessorForHeaders{},
		&MockProtocolMessage{api: &spectypes.Api{Name: "net_version"}, unavailableExtensions: unavailable},
		"net_version", nil, true)
	return relayResult.Reply.Metadata
}

func TestExtensionUnavailableHeader_NamesTheDroppedExtension(t *testing.T) {
	metadata := appendHeadersFor(t, &RPCSmartRouterServer{}, []string{"archive"})

	require.Equal(t, []string{"archive"}, headerValues(metadata, common.EXTENSION_UNAVAILABLE_HEADER_NAME))
	require.Equal(t, []string{"lava@provider1"}, headerValues(metadata, common.PROVIDER_ADDRESS_HEADER_NAME),
		"the request is still served, and still says by whom")
}

func TestExtensionUnavailableHeader_ListsEveryDroppedExtension(t *testing.T) {
	metadata := appendHeadersFor(t, &RPCSmartRouterServer{}, []string{"archive", "debug"})

	require.Equal(t, []string{"archive,debug"}, headerValues(metadata, common.EXTENSION_UNAVAILABLE_HEADER_NAME))
}

// The ordinary request — nothing requested, or everything requested was honoured — carries no
// such header, so its presence alone means "served without what you asked for".
func TestExtensionUnavailableHeader_AbsentWhenNothingWasDropped(t *testing.T) {
	metadata := appendHeadersFor(t, &RPCSmartRouterServer{}, nil)

	require.Empty(t, headerValues(metadata, common.EXTENSION_UNAVAILABLE_HEADER_NAME))
}

// The operator log is once per extension; every request still gets the header.
func TestExtensionUnavailableHeader_WarnsOncePerExtensionButHeadersEveryRequest(t *testing.T) {
	srv := &RPCSmartRouterServer{}

	for range 3 {
		metadata := appendHeadersFor(t, srv, []string{"archive"})
		require.Equal(t, []string{"archive"}, headerValues(metadata, common.EXTENSION_UNAVAILABLE_HEADER_NAME))
	}

	warned := 0
	srv.warnedUnavailableExtensions.Range(func(_, _ any) bool { warned++; return true })
	require.Equal(t, 1, warned, "one extension, one warning, however many requests")
}

// The router adds archive on its own to an eth_call deep behind the head. The caller did not ask
// for it, so on a fleet without an archive node that must not come back as the header: it names
// only what the caller asked for with lava-extension. Driven through ParseRelay, so the caller's
// directive and the router's promotion arrive the way a real request brings them.
func TestExtensionUnavailableHeader_NamesOnlyWhatTheCallerAskedFor(t *testing.T) {
	const tip = 1_000_000
	ctx := context.Background()
	// No node behind this parser declares archive and no policy is set, as on a router with no
	// archive node.
	chainState := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	chainState.SetLatestBlock(tip)
	srv := &RPCSmartRouterServer{
		chainParser:    ethJsonRPCParser(t),
		chainState:     chainState,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
	// The promotion needs a known head; without one the deep call below would not be promoted
	// and the first case would pass without testing anything.
	require.Equal(t, uint64(tip), srv.getLatestBlockAllowStale())

	ethCallAt := func(block uint64) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x1111111111111111111111111111111111111111","data":"0x"},"0x%x"]}`, block)
	}
	askForArchive := []pairingtypes.Metadata{{Name: common.EXTENSION_OVERRIDE_HEADER_NAME, Value: extensionslib.ArchiveExtension}}

	cases := []struct {
		name     string
		body     string
		metadata []pairingtypes.Metadata
		reported []string
	}{
		{"the router's own archive promotion is not reported", ethCallAt(tip - 1000), nil, nil},
		{"the caller's lava-extension is still reported", ethCallAt(tip), askForArchive, []string{extensionslib.ArchiveExtension}},
		{"asked for by the caller and the router, reported once", ethCallAt(tip - 1000), askForArchive, []string{extensionslib.ArchiveExtension}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			protocolMessage, err := srv.ParseRelay(ctx, "", tc.body, http.MethodPost, "test-dapp", "127.0.0.1", tc.metadata)
			require.NoError(t, err)
			require.Empty(t, protocolMessage.GetExtensions(), "no node offers archive, so it is never applied")

			relayResult := &common.RelayResult{
				ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
				Reply:        &pairingtypes.RelayReply{},
			}
			srv.appendHeadersToRelayResult(ctx, relayResult, 0, &MockRelayProcessorForHeaders{}, protocolMessage,
				protocolMessage.GetApi().GetName(), nil, true)
			header := headerValues(relayResult.Reply.Metadata, common.EXTENSION_UNAVAILABLE_HEADER_NAME)

			if tc.reported == nil {
				require.Empty(t, protocolMessage.GetUnavailableExtensions(), "the caller asked for nothing")
				require.Empty(t, header, "the caller asked for nothing, so there is nothing to report")
				return
			}
			require.Equal(t, tc.reported, protocolMessage.GetUnavailableExtensions())
			require.Equal(t, []string{strings.Join(tc.reported, ",")}, header)
		})
	}
}
