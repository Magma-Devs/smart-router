package rpcsmartrouter

import (
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// Helpers shared by the reply-header tests of this package. The filter's own tests live
// with it in protocol/common (reply_headers_test.go); direct_rpc_reply_headers_test.go
// drives it through the relay senders against a real upstream, and
// rest_integration_test.go through the real LAVA and APT1 specs.

func metadataNames(md []pairingtypes.Metadata) []string {
	names := make([]string, 0, len(md))
	for _, m := range md {
		names = append(names, m.Name)
	}
	return names
}

// aptosLedgerStateHeaders is what api.mainnet.aptoslabs.com sent on GET /v1/ and on
// POST /v1/view (2026-10-04): the seven headers aptos-rest-client's State::from_headers
// requires on every successful reply. Spelled as Go's HTTP client canonicalises them
// (the node sends them lowercase); the spec names them lowercase, and the filter matches
// case-insensitively.
var aptosLedgerStateHeaders = map[string]string{
	"X-Aptos-Chain-Id":              "1",
	"X-Aptos-Ledger-Version":        "7479307940",
	"X-Aptos-Ledger-Oldest-Version": "7329507940",
	"X-Aptos-Ledger-Timestampusec":  "1791116237366446",
	"X-Aptos-Epoch":                 "17518",
	"X-Aptos-Block-Height":          "1089989182",
	"X-Aptos-Oldest-Block-Height":   "1065302910",
}

// aptosGETDirectives mirrors the APT1 spec's GET collection: seven pass_reply, the per-node
// timestamp pass_ignore, and the pagination cursor. The POST collection declares none.
var aptosGETDirectives = []*spectypes.Header{
	{Name: "x-aptos-ledger-version", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-block-height", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-ledger-oldest-version", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-oldest-block-height", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-chain-id", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-epoch", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-ledger-timestampusec", Kind: spectypes.Header_pass_ignore},
	{Name: "x-aptos-cursor", Kind: spectypes.Header_pass_reply},
}
