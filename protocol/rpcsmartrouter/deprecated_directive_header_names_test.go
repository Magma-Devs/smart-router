package rpcsmartrouter

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Version 1.7.0 renamed the client directives from lava-* to router-* with no alias. A request that
// still sent an old name got no error and lost its directive: one production client asked for
// cross-validation under the old names and was answered by a single provider (MAG-5817). These tests
// hold the fix: every directive a client sends is read under its old name too.

// The table must name every client directive, each under the old spelling of its own name. Ranging
// over the registry means a directive added later fails here until someone decides whether it needs
// an old name. The monitoring marker is the one exception: only our own probes send it.
func TestDeprecatedDirectiveNamesCoverEveryClientDirective(t *testing.T) {
	covered := map[string]string{}
	for deprecatedName, currentName := range common.DEPRECATED_DIRECTIVE_HEADER_NAMES {
		require.Equal(t, strings.ToLower(deprecatedName), deprecatedName, "header names are matched in lower case")
		require.Contains(t, common.SPECIAL_DIRECTIVE_HEADERS, currentName,
			"%s must map to a registered directive", deprecatedName)
		require.NotContains(t, common.SPECIAL_DIRECTIVE_HEADERS, deprecatedName,
			"%s is a current directive name and cannot also be an old one", deprecatedName)
		require.Equal(t, "lava-"+strings.TrimPrefix(currentName, "router-"), deprecatedName,
			"the old name of %s is the same name under the lava- prefix", currentName)
		require.NotContains(t, covered, currentName, "%s has two old names", currentName)
		covered[currentName] = deprecatedName
	}
	for currentName := range common.SPECIAL_DIRECTIVE_HEADERS {
		if currentName == common.INTERNAL_PROBE_HEADER_NAME {
			require.NotContains(t, covered, currentName, "the monitoring marker is ours and has no old name")
			continue
		}
		require.Contains(t, covered, currentName, "client directive %s has no old name", currentName)
	}
	require.Len(t, common.DEPRECATED_DIRECTIVE_HEADER_NAMES, 10, "version 1.7.0 renamed 10 client directives")
}

func TestDirectiveHeadersReadsDeprecatedNames(t *testing.T) {
	rpcss := &RPCSmartRouterServer{}
	ordinary := pairingtypes.Metadata{Name: "X-Ordinary-Header", Value: "kept"}

	// An HTTP listener hands the names over as the client wrote them, so the match cannot depend on
	// letter case.
	spellings := map[string]func(string) string{
		"lower case": strings.ToLower,
		"upper case": strings.ToUpper,
		"title case": func(name string) string { return http.CanonicalHeaderKey(name) },
	}
	for deprecatedName, currentName := range common.DEPRECATED_DIRECTIVE_HEADER_NAMES {
		for spelling, spell := range spellings {
			t.Run(deprecatedName+"/"+spelling, func(t *testing.T) {
				forwarded, directives := rpcss.DirectiveHeaders([]pairingtypes.Metadata{
					{Name: spell(deprecatedName), Value: "directive-value"},
					ordinary,
				})
				require.Equal(t, map[string]string{currentName: "directive-value"}, directives,
					"an old name must be read as the current one, and only under the current one")
				require.Equal(t, []pairingtypes.Metadata{ordinary}, forwarded,
					"the old name is the router's own and must not reach the node; an ordinary header must")
			})
		}
	}

	t.Run("a name that only looks like an old one is an ordinary header", func(t *testing.T) {
		unknown := pairingtypes.Metadata{Name: "lava-not-a-directive", Value: "x"}
		forwarded, directives := rpcss.DirectiveHeaders([]pairingtypes.Metadata{unknown})
		require.Empty(t, directives)
		require.Equal(t, []pairingtypes.Metadata{unknown}, forwarded)
	})
}

// A client in the middle of moving to the new names may send both spellings. The current name wins
// whichever came first, and neither spelling reaches the node.
func TestDirectiveHeadersCurrentNameWinsOverDeprecatedName(t *testing.T) {
	rpcss := &RPCSmartRouterServer{}
	for deprecatedName, currentName := range common.DEPRECATED_DIRECTIVE_HEADER_NAMES {
		oldSpelling := pairingtypes.Metadata{Name: deprecatedName, Value: "from-old-name"}
		newSpelling := pairingtypes.Metadata{Name: currentName, Value: "from-current-name"}
		orders := map[string][]pairingtypes.Metadata{
			"old name first":     {oldSpelling, newSpelling},
			"current name first": {newSpelling, oldSpelling},
		}
		for order, metadata := range orders {
			t.Run(currentName+"/"+order, func(t *testing.T) {
				forwarded, directives := rpcss.DirectiveHeaders(metadata)
				require.Equal(t, map[string]string{currentName: "from-current-name"}, directives)
				require.Empty(t, forwarded)
			})
		}
	}

	t.Run("two directives, one under each spelling, both apply", func(t *testing.T) {
		_, directives := rpcss.DirectiveHeaders([]pairingtypes.Metadata{
			{Name: common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS, Value: "3"},
			{Name: "lava-cross-validation-agreement-threshold", Value: "2"},
		})
		require.Equal(t, map[string]string{
			common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
			common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
		}, directives)
	})
}

// The production case, through ParseRelay, the entry point every HTTP and gRPC request takes: a
// request that asks for cross-validation under the old names is a cross-validation request with the
// numbers it asked for, the old names never reach the metadata the node and the cache key are built
// from, and an old name shares its cache entry with the current one.
func TestParseRelayAppliesDeprecatedDirectiveNames(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	rpcss := &RPCSmartRouterServer{
		chainParser:    chainParser,
		listenEndpoint: &routersession.RPCEndpoint{ChainID: "LAVA", ApiInterface: spectypes.APIInterfaceRest},
	}

	const apiURL = "/cosmos/base/tendermint/v1beta1/blocks/latest"
	parse := func(t *testing.T, metadata ...pairingtypes.Metadata) chainlib.ProtocolMessage {
		t.Helper()
		protocolMessage, err := rpcss.ParseRelay(ctx, apiURL, "", http.MethodGet, "test-dapp", "127.0.0.1", metadata)
		require.NoError(t, err)
		return protocolMessage
	}
	hashOf := func(t *testing.T, protocolMessage chainlib.ProtocolMessage) []byte {
		t.Helper()
		hashKey, _, err := protocolMessage.HashCacheRequest("LAVA")
		require.NoError(t, err)
		return hashKey
	}

	t.Run("cross-validation asked for under the old names is cross-validation", func(t *testing.T) {
		underOldNames := parse(t,
			pairingtypes.Metadata{Name: "Lava-Cross-Validation-Max-Participants", Value: "3"},
			pairingtypes.Metadata{Name: "Lava-Cross-Validation-Agreement-Threshold", Value: "2"},
		)
		params, asked, err := underOldNames.GetCrossValidationParameters()
		require.NoError(t, err)
		require.True(t, asked, "the request asked for cross-validation and must not be routed as an ordinary one")
		require.Equal(t, 3, params.MaxParticipants)
		require.Equal(t, 2, params.AgreementThreshold)

		underCurrentNames := parse(t,
			pairingtypes.Metadata{Name: common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS, Value: "3"},
			pairingtypes.Metadata{Name: common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD, Value: "2"},
		)
		currentParams, currentAsked, err := underCurrentNames.GetCrossValidationParameters()
		require.NoError(t, err)
		require.True(t, currentAsked)
		require.Equal(t, currentParams, params, "both spellings must give the same cross-validation request")

		_, plainAsked, err := parse(t).GetCrossValidationParameters()
		require.NoError(t, err)
		require.False(t, plainAsked, "a request with neither spelling is an ordinary one")
	})

	t.Run("an old name never reaches the metadata the node and the cache key are built from", func(t *testing.T) {
		for deprecatedName := range common.DEPRECATED_DIRECTIVE_HEADER_NAMES {
			value := "true"
			switch deprecatedName {
			case "lava-cross-validation-max-participants", "lava-cross-validation-agreement-threshold":
				continue // need each other; covered above
			case "lava-relay-timeout":
				value = "5s"
			case "lava-extension":
				value = "archive"
			}
			withOldName := parse(t, pairingtypes.Metadata{Name: deprecatedName, Value: value})
			for _, metadata := range withOldName.RelayPrivateData().Metadata {
				require.NotEqual(t, deprecatedName, strings.ToLower(metadata.Name),
					"%s survived into RelayPrivateData and would be sent to the node and hashed into the cache key", deprecatedName)
			}
			require.Equal(t, value, withOldName.GetDirectiveHeaders()[common.DEPRECATED_DIRECTIVE_HEADER_NAMES[deprecatedName]])
		}
	})

	t.Run("an old name and the current name share one cache entry", func(t *testing.T) {
		plain := hashOf(t, parse(t))
		// A directive that leaves the key alone, and one that moves the request to its own lane.
		require.Equal(t, plain, hashOf(t, parse(t, pairingtypes.Metadata{Name: "lava-debug-relay", Value: "true"})))

		oldName := hashOf(t, parse(t, pairingtypes.Metadata{Name: "lava-extension", Value: "archive"}))
		currentName := hashOf(t, parse(t, pairingtypes.Metadata{Name: common.EXTENSION_OVERRIDE_HEADER_NAME, Value: "archive"}))
		require.Equal(t, currentName, oldName, "the same request under either spelling must hit the same cache entry")
		// Sensitivity control: the extension directive does move the key, so the equality above
		// compares two keys that the directive shaped, not two untouched ones.
		require.NotEqual(t, plain, currentName, "the extension directive must change the key, or the equality above proves nothing")
	})
}
