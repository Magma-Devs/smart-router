package chainlib

import (
	"net/http"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

func directiveNames(directives []*spectypes.Header) []string {
	names := make([]string, 0, len(directives))
	for _, d := range directives {
		names = append(names, strings.ToLower(d.Name))
	}
	return names
}

// The reply-direction directives are resolved for the whole API interface (MAG-3104):
// APT1 declares its ledger-state headers on the GET collection only, Aptos nodes send
// them on POST too, and the Aptos Rust SDK fails a call that comes back without them.
func TestReplyHeaderDirectivesSpanTheInterfaceAptos(t *testing.T) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "APT1")
	require.NoError(t, err)
	parser, err := NewRestChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	require.Equal(t, []string{
		"x-aptos-ledger-version", "x-aptos-block-height", "x-aptos-ledger-oldest-version", "x-aptos-oldest-block-height",
		"x-aptos-chain-id", "x-aptos-epoch", "x-aptos-ledger-timestampusec", "x-aptos-cursor",
	}, directiveNames(parser.ReplyHeaderDirectives()), "all eight, the pass_ignore timestamp included, in spec order")

	view, err := parser.ParseMsg("/view", []byte(`{"function":"0x1::chain_id::get","type_arguments":[],"arguments":[]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	require.Empty(t, view.GetApiCollection().Headers, "the POST collection itself declares none; the interface's union is what covers it")
}

func TestReplyHeaderDirectivesKeepSpecOrderAndSkipWhatDoesNotPassOnReply(t *testing.T) {
	spec := spectypes.Spec{
		Enabled: true,
		Index:   "TEST",
		ApiCollections: []*spectypes.ApiCollection{
			{
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodGet},
				Headers: []*spectypes.Header{
					{Name: "x-reply", Kind: spectypes.Header_pass_reply},
					{Name: "x-send-only", Kind: spectypes.Header_pass_send},
					{Name: "x-nullified", Kind: spectypes.Header_pass_nullify},
					{Name: "x-overridden", Kind: spectypes.Header_pass_override},
					{Name: "x-ignored", Kind: spectypes.Header_pass_ignore},
				},
			},
			{
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodPost},
				Headers: []*spectypes.Header{
					{Name: "X-Reply", Kind: spectypes.Header_pass_both}, // same name, other spelling: once
					{Name: "x-both", Kind: spectypes.Header_pass_both},
				},
			},
			{
				Enabled:        false,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodPut},
				Headers:        []*spectypes.Header{{Name: "x-disabled-collection", Kind: spectypes.Header_pass_reply}},
			},
			{
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC, Type: http.MethodPost},
				Headers:        []*spectypes.Header{{Name: "x-other-interface", Kind: spectypes.Header_pass_reply}},
			},
		},
	}
	parser, err := NewRestChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	require.Equal(t, []string{"x-reply", "x-ignored", "x-both"}, directiveNames(parser.ReplyHeaderDirectives()))
	require.Equal(t, "x-reply", parser.ReplyHeaderDirectives()[0].Name, "the first declaration's spelling is kept")
}

func TestReplyHeaderDirectivesAreNilWithoutAny(t *testing.T) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "ETH1")
	require.NoError(t, err)
	parser, err := NewJrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)
	require.Nil(t, parser.ReplyHeaderDirectives())
}

func TestHeaderPassesOnReply(t *testing.T) {
	passes := map[spectypes.Header_HeaderType]bool{
		spectypes.Header_pass_send:     false,
		spectypes.Header_pass_reply:    true,
		spectypes.Header_pass_both:     true,
		spectypes.Header_pass_ignore:   true,
		spectypes.Header_pass_nullify:  false,
		spectypes.Header_pass_override: false,
	}
	require.Len(t, passes, len(spectypes.Header_HeaderType_name), "every kind has a verdict")
	for kind, want := range passes {
		require.Equalf(t, want, HeaderPassesOnReply(kind), "%s", kind)
	}
}
