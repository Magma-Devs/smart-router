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

	t.Run("a spec that declares it keeps its say: pass_ignore still drops it", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{
			{Name: "content-type", ConnectionType: http.MethodPost}: {Name: "content-type", Kind: spectypes.Header_pass_ignore},
		}}
		filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(http.MethodPost), spectypes.Header_pass_send)
		require.Empty(t, filtered)
	})

	t.Run("a spec pass_override still wins over the client", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{
			{Name: "content-type", ConnectionType: http.MethodPost}: {Name: "content-type", Kind: spectypes.Header_pass_override, Value: "application/json"},
		}}
		filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(http.MethodPost), spectypes.Header_pass_send)
		require.Equal(t, "application/json", effectiveHeaders(filtered).Get("Content-Type"))
		require.Len(t, filtered, 1, "the declared name is the spec's alone; the client's entry is not carried alongside it")
	})

	t.Run("a spec pass_nullify still removes it", func(t *testing.T) {
		bcp := &BaseChainParser{headers: map[ApiKey]*spectypes.Header{
			{Name: "content-type", ConnectionType: http.MethodPost}: {Name: "content-type", Kind: spectypes.Header_pass_nullify},
		}}
		filtered, _, _ := bcp.HandleHeaders(clientContentType, restCollection(http.MethodPost), spectypes.Header_pass_send)
		require.Empty(t, effectiveHeaders(filtered).Values("Content-Type"))
	})
}

// TestHandleHeaders_ProjectIDLabelsAndNeverRoutes pins MAG-4081: the caller's project-id header
// names the customer a request is counted under, and nothing else. Every relay input downstream
// of the listener — the upstream request's headers and the cache key, both built from this
// function's output — must come out identical whatever project-id says, so two customers sending
// the same call share one cache entry and one routing decision, and a provider never sees the id.
func TestHandleHeaders_ProjectIDLabelsAndNeverRoutes(t *testing.T) {
	projectID := func(value string) pairingtypes.Metadata {
		return pairingtypes.Metadata{Name: ProjectIDHeader, Value: value}
	}
	// A header the spec does forward, so the comparison below cannot pass on an empty result.
	forwarded := pairingtypes.Metadata{Name: "x-cosmos-block-height", Value: "1000"}
	// CARDANO declares project_id (underscore, the Blockfrost key) pass_send. Exact-name matching
	// must keep that directive from reaching the hyphenated label header.
	declare := func(method string) map[ApiKey]*spectypes.Header {
		return map[ApiKey]*spectypes.Header{
			{Name: "x-cosmos-block-height", ConnectionType: method}: {Name: "x-cosmos-block-height", Kind: spectypes.Header_pass_send},
			{Name: "project_id", ConnectionType: method}:            {Name: "project_id", Kind: spectypes.Header_pass_send},
		}
	}

	for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC, spectypes.APIInterfaceRest, spectypes.APIInterfaceGrpc, spectypes.APIInterfaceGraphQL} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			bcp := &BaseChainParser{headers: declare(method)}
			collection := &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{ApiInterface: apiInterface, Type: method}}
			filter := func(metadata ...pairingtypes.Metadata) []pairingtypes.Metadata {
				filtered, _, _ := bcp.HandleHeaders(metadata, collection, spectypes.Header_pass_send)
				return filtered
			}

			without := filter(forwarded)
			require.Equal(t, []pairingtypes.Metadata{forwarded}, without, "%s %s: control — the declared header is forwarded", apiInterface, method)
			for _, customer := range []string{"acme", "globex"} {
				require.Equal(t, without, filter(forwarded, projectID(customer)),
					"%s %s: project-id must leave the forwarded headers, and so the upstream request and the cache key, unchanged", apiInterface, method)
				require.Equal(t, without, filter(projectID(customer), forwarded), "%s %s: whatever its position", apiInterface, method)
			}
		}
	}
}
