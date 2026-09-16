package chainlib

import (
	"net/http"
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// effectiveHeaders replays HandleHeaders' output the way the HTTP transports consume it
// (HTTPDirectRPCConnection.DoHTTPRequest and RestChainProxy.SendNodeMsg): entries are
// applied in order, a non-empty value sets the header and an empty value deletes it. The
// precedence between a client header and a spec directive lives in that ordering, so the
// assertions below check what reaches the wire, not the raw slice.
func effectiveHeaders(metadata []pairingtypes.Metadata) http.Header {
	header := http.Header{}
	for _, entry := range metadata {
		if entry.Value == "" {
			header.Del(entry.Name)
		} else {
			header.Set(entry.Name, entry.Value)
		}
	}
	return header
}

func restCollection(method string) *spectypes.ApiCollection {
	return &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: method}}
}

// TestHandleHeaders_RestContentTypeIsForwardedWithoutDirective pins MAG-2745: on the REST
// interface the client's Content-Type describes a body the router forwards byte-for-byte,
// so it is forwarded without a spec directive, on the methods that carry a body only, and
// only on the request direction. A spec directive for the same name keeps the last word.
func TestHandleHeaders_RestContentTypeIsForwardedWithoutDirective(t *testing.T) {
	const formURLEncoded = "application/x-www-form-urlencoded"
	clientContentType := []pairingtypes.Metadata{{Name: "Content-Type", Value: formURLEncoded}}

	t.Run("forwarded on REST methods that carry a body", func(t *testing.T) {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
			bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{}}
			filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(method), spectypes.Header_pass_send)
			require.Equal(t, clientContentType, filtered, "method %s must forward the client's content-type unchanged", method)
		}
	})

	t.Run("dropped on REST methods without a body", func(t *testing.T) {
		// A GET has no body to describe, and forwarded metadata is hashed into the
		// cache key, so letting the header through would only split cache lanes.
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{}}
			filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(method), spectypes.Header_pass_send)
			require.Empty(t, filtered, "method %s must not forward content-type", method)
		}
	})

	t.Run("dropped on interfaces where the router authors the body", func(t *testing.T) {
		for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC, spectypes.APIInterfaceGrpc} {
			bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{}}
			collection := &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{ApiInterface: apiInterface, Type: http.MethodPost}}
			filtered, _, _ := bcp.HandleHeaders(clientContentType, collection, spectypes.Header_pass_send)
			require.Empty(t, filtered, "interface %s must keep its own content-type", apiInterface)
		}
	})

	t.Run("request direction only", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{}}
		filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(http.MethodPost), spectypes.Header_pass_reply)
		require.Empty(t, filtered)
	})

	t.Run("only content-type, and only when it carries a value", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{}}
		undeclared := []pairingtypes.Metadata{
			{Name: "x-custom", Value: "1"},
			{Name: "Content-Type", Value: ""},
		}
		filtered, _, _ := bcp.HandleHeaders(undeclared, restCollection(http.MethodPost), spectypes.Header_pass_send)
		require.Empty(t, filtered, "an undeclared header other than content-type, or an empty content-type, must still be dropped")
	})

	t.Run("a spec pass_send declaration forwards it once, not twice", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{
			{Name: "content-type", ConnectionType: http.MethodPost}: {Name: "content-type", Kind: spectypes.Header_pass_send},
		}}
		filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(http.MethodPost), spectypes.Header_pass_send)
		require.Equal(t, clientContentType, filtered)
	})

	t.Run("a spec pass_override still wins over the client", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{
			{Name: "content-type", ConnectionType: http.MethodPost}: {Name: "content-type", Kind: spectypes.Header_pass_override, Value: "application/json"},
		}}
		filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(http.MethodPost), spectypes.Header_pass_send)
		require.Equal(t, "application/json", effectiveHeaders(filtered).Get("Content-Type"))
	})

	t.Run("a spec pass_nullify still removes it", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{
			{Name: "content-type", ConnectionType: http.MethodPost}: {Name: "content-type", Kind: spectypes.Header_pass_nullify},
		}}
		filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(http.MethodPost), spectypes.Header_pass_send)
		require.Empty(t, effectiveHeaders(filtered).Values("Content-Type"))
	})
}
