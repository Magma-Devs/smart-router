package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
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
