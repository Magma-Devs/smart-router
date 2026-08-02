package performance

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strconv"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

func TestSanitizeForeignCacheReply_StripsIdentityMetadata(t *testing.T) {
	reply := &pairingtypes.RelayReply{
		Data:                  []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`),
		Sig:                   []byte{0x01},
		SigBlocks:             []byte{0x02},
		LatestBlock:           1234,
		FinalizedBlocksHashes: []byte(`{"1200":"0xabc"}`),
		Metadata: []pairingtypes.Metadata{
			{Name: "Lava-Provider-Address", Value: "lava@provider1"},
			{Name: "lava-cross-validation-all-providers", Value: "lava@p1,lava@p2"},
			{Name: "LAVA-RETRIES", Value: "2"},
			{Name: "Provider-Latest-Block", Value: "1234"},
			{Name: "Smart-Router-Version", Value: "v1.3.0"},
			{Name: "Content-Type", Value: "application/json"},
			{Name: "X-Node-Custom", Value: "geth/1.14"},
		},
	}

	SanitizeForeignCacheReply(reply)

	require.Nil(t, reply.Sig)
	require.Nil(t, reply.SigBlocks)
	names := make([]string, 0, len(reply.Metadata))
	for _, md := range reply.Metadata {
		names = append(names, md.Name)
	}
	require.ElementsMatch(t, []string{"Content-Type", "X-Node-Custom"}, names)
	// non-identity payload fields are untouched
	require.Equal(t, int64(1234), reply.LatestBlock)
	require.Equal(t, []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), reply.Data)
	require.NotEmpty(t, reply.FinalizedBlocksHashes)
}

func TestSanitizeForeignCacheReply_NilAndEmptySafe(t *testing.T) {
	require.NotPanics(t, func() { SanitizeForeignCacheReply(nil) })
	reply := &pairingtypes.RelayReply{}
	require.NotPanics(t, func() { SanitizeForeignCacheReply(reply) })
	require.Empty(t, reply.Metadata)
}

func TestIsIdentityHeader(t *testing.T) {
	identity := []string{
		common.PROVIDER_ADDRESS_HEADER_NAME,
		common.RETRY_COUNT_HEADER_NAME,
		common.PROVIDER_LATEST_BLOCK_HEADER_NAME,
		common.SMART_ROUTER_VERSION_HEADER_NAME,
		"lava-any-future-header",
		"LAVA-PROVIDER-ADDRESS", // case-insensitive
	}
	for _, name := range identity {
		require.True(t, IsIdentityHeader(name), "expected identity header: %s", name)
	}
	nonIdentity := []string{"Content-Type", "Content-Length", "X-Node-Custom", "Server"}
	for _, name := range nonIdentity {
		require.False(t, IsIdentityHeader(name), "expected non-identity header: %s", name)
	}
}

// requestSideOnly lists header constants that are read from inbound requests and never
// minted onto responses — the identity rule does not apply to them. Adding a new
// non-"lava-" header constant to protocol/common forces a choice here: classify it as
// identity (extend IsIdentityHeader) or as request-side (extend this list).
var requestSideOnly = map[string]struct{}{
	strings.ToLower(common.IP_FORWARDING_HEADER_NAME): {},
}

// TestResponseHeaderConstantsAreIdentityClassified is the guard from
// docs/SECONDARY-CACHE-DESIGN.md §4 (T11b): the sanitization denylist is a bounded
// enumeration, and this test is what bounds it. Every string constant in
// protocol/common whose name contains HEADER must be either identity-classified by
// IsIdentityHeader or explicitly declared request-side-only above — otherwise a newly
// added header could leak through SanitizeForeignCacheReply unclassified.
func TestResponseHeaderConstantsAreIdentityClassified(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "../common", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)

	checked := 0
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				genDecl, ok := decl.(*ast.GenDecl)
				if !ok || genDecl.Tok != token.CONST {
					continue
				}
				for _, spec := range genDecl.Specs {
					valueSpec, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, nameIdent := range valueSpec.Names {
						if !strings.Contains(nameIdent.Name, "HEADER") {
							continue
						}
						if i >= len(valueSpec.Values) {
							continue
						}
						lit, ok := valueSpec.Values[i].(*ast.BasicLit)
						if !ok || lit.Kind != token.STRING {
							continue
						}
						value, unquoteErr := strconv.Unquote(lit.Value)
						require.NoError(t, unquoteErr)
						if _, isRequestSide := requestSideOnly[strings.ToLower(value)]; isRequestSide {
							continue
						}
						checked++
						require.True(t, IsIdentityHeader(value),
							"header constant %s=%q in protocol/common is neither identity-classified (lava- prefix or named exception in IsIdentityHeader) nor listed as request-side-only; classify it so SanitizeForeignCacheReply cannot silently leak it (docs/SECONDARY-CACHE-DESIGN.md §4)",
							nameIdent.Name, value)
					}
				}
			}
		}
	}
	require.Greater(t, checked, 10, "guard scanned suspiciously few header constants — did they move out of protocol/common?")
}
