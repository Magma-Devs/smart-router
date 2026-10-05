package rpcsmartrouter

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/prometheus/client_golang/prometheus"
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

// The extension name arrives in a fiber request header, whose string aliases fasthttp's
// per-connection header buffer: the next request on the same keep-alive connection rewrites those
// bytes in place. The warn-once register outlives the request, so a key that still aliased the
// buffer would silently change text — "archive" becoming "invalid" — and the register would
// re-warn for archive and claim to have warned for a name nobody asked about.
//
// Literal strings cannot reproduce this; it takes a real listener and ONE connection. The handler
// does what the JSON-RPC listener does (GetReqHeaders, then strings.Join per header, which returns
// a lone value itself rather than a copy), on an app configured like the router's (not Immutable).
// Both values are seven bytes and lava-extension is the only non-special header, so the second
// request's value lands in the very slot the first one's did.
func TestExtensionUnavailableHeader_WarnRegisterSurvivesKeepAliveBufferReuse(t *testing.T) {
	const tip = 1_000_000
	chainState := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	chainState.SetLatestBlock(tip)
	srv := &RPCSmartRouterServer{
		chainParser:    ethJsonRPCParser(t),
		chainState:     chainState,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}

	app := fiber.New(fiber.Config{ReadBufferSize: 128 * 1024, WriteBufferSize: 128 * 1024})
	app.Post("/", func(c *fiber.Ctx) error {
		var headers []pairingtypes.Metadata
		for name, values := range c.GetReqHeaders() {
			headers = append(headers, pairingtypes.Metadata{Name: name, Value: strings.Join(values, ", ")})
		}
		ctx := context.Background()
		protocolMessage, err := srv.ParseRelay(ctx, "", string(c.Body()), http.MethodPost, "test-dapp", "127.0.0.1", headers)
		if err != nil {
			return err
		}
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
			Reply:        &pairingtypes.RelayReply{},
		}
		srv.appendHeadersToRelayResult(ctx, relayResult, 0, &MockRelayProcessorForHeaders{}, protocolMessage,
			protocolMessage.GetApi().GetName(), nil, true)
		for _, md := range relayResult.Reply.Metadata {
			c.Set(md.Name, md.Value)
		}
		return c.SendString(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(listener) }()
	t.Cleanup(func() { _ = app.Shutdown() })

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	send := func(extension string) *http.Response {
		t.Helper()
		body := `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`
		request := fmt.Sprintf("POST / HTTP/1.1\r\nHost: router\r\nContent-Type: application/json\r\nLava-Extension: %s\r\nContent-Length: %d\r\n\r\n%s",
			extension, len(body), body)
		_, err := conn.Write([]byte(request))
		require.NoError(t, err)
		response, err := http.ReadResponse(reader, nil)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, response.Body)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.Equal(t, http.StatusOK, response.StatusCode)
		return response
	}

	first := send(extensionslib.ArchiveExtension)
	require.Equal(t, extensionslib.ArchiveExtension, first.Header.Get(common.EXTENSION_UNAVAILABLE_HEADER_NAME),
		"no node offers archive, so the first reply reports it")
	// Not an extension the spec knows, so it is dropped before it can be reported; its only effect
	// is to overwrite the header slot the first request's value lived in.
	second := send("invalid")
	require.Empty(t, second.Header.Get(common.EXTENSION_UNAVAILABLE_HEADER_NAME))

	var warned []string
	srv.warnedUnavailableExtensions.Range(func(key, _ any) bool {
		warned = append(warned, key.(string))
		return true
	})
	require.Equal(t, []string{extensionslib.ArchiveExtension}, warned,
		"the register must own its keys; one aliasing the connection's header buffer reads back as the next request's value")
}

// extensionUnavailableCount reads smartrouter_extension_unavailable_total for spec from the default
// registry, per extension label.
func extensionUnavailableCount(t *testing.T, spec string) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	counts := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "smartrouter_extension_unavailable_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["spec"] == spec {
				counts[labels["extension"]] = metric.GetCounter().GetValue()
			}
		}
	}
	return counts
}

// The operator counter fires on every request that carries the header, where the WARN fires once.
// Its extension label takes only names the spec defines: a made-up lava-extension is dropped before
// it can be recorded, so callers cannot mint series.
func TestExtensionUnavailableHeader_CountsEveryRequestAndOnlySpecExtensions(t *testing.T) {
	// A spec label no other test uses, so the default registry's series are this test's alone.
	const spec = "ETH1-MAG3935-COUNTER"
	chainState := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	chainState.SetLatestBlock(1_000_000)
	srv := &RPCSmartRouterServer{
		chainParser:                ethJsonRPCParser(t),
		chainState:                 chainState,
		listenEndpoint:             &lavasession.RPCEndpoint{ChainID: spec, ApiInterface: spectypes.APIInterfaceJsonRPC},
		smartRouterEndpointMetrics: metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{}),
	}
	relay := func(extensions string) []string {
		t.Helper()
		ctx := context.Background()
		protocolMessage, err := srv.ParseRelay(ctx, "", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`, http.MethodPost, "test-dapp", "127.0.0.1",
			[]pairingtypes.Metadata{{Name: common.EXTENSION_OVERRIDE_HEADER_NAME, Value: extensions}})
		require.NoError(t, err)
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@provider1"},
			Reply:        &pairingtypes.RelayReply{},
		}
		srv.appendHeadersToRelayResult(ctx, relayResult, 0, &MockRelayProcessorForHeaders{}, protocolMessage,
			protocolMessage.GetApi().GetName(), nil, true)
		return headerValues(relayResult.Reply.Metadata, common.EXTENSION_UNAVAILABLE_HEADER_NAME)
	}

	// The default registry is process-wide and outlives a -count run, so measure the change.
	before := extensionUnavailableCount(t, spec)[extensionslib.ArchiveExtension]
	for range 3 {
		require.Equal(t, []string{extensionslib.ArchiveExtension}, relay(extensionslib.ArchiveExtension))
	}
	require.Empty(t, relay("made-up-extension"), "not an extension the spec defines, so nothing is reported")
	require.Equal(t, []string{extensionslib.ArchiveExtension}, relay("made-up-extension,"+extensionslib.ArchiveExtension))

	require.Equal(t, map[string]float64{extensionslib.ArchiveExtension: before + 4}, extensionUnavailableCount(t, spec),
		"one count per request that named archive, and no series for a name the spec does not define")
	warned := 0
	srv.warnedUnavailableExtensions.Range(func(_, _ any) bool { warned++; return true })
	require.Equal(t, 1, warned, "the WARN stays once per extension")
}
