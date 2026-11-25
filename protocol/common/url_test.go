package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinURLPath(t *testing.T) {
	tests := []struct {
		name        string
		base        string
		path        string
		wantURL     string
		wantErr     bool
		description string
	}{
		{
			name:        "absolute path preserves base path (gateway)",
			base:        "https://gateway.example.com:443/gateway/cosmoshub/rest/YOUR_GATEWAY_KEY",
			path:        "/cosmos/bank/v1beta1/balances/cosmos1ax3pa3eg0k5wnmuj8v3k2yd87a4ka5g9ymwdu3",
			wantURL:     "https://gateway.example.com:443/gateway/cosmoshub/rest/YOUR_GATEWAY_KEY/cosmos/bank/v1beta1/balances/cosmos1ax3pa3eg0k5wnmuj8v3k2yd87a4ka5g9ymwdu3",
			wantErr:     false,
			description: "base path must not be replaced by ResolveReference",
		},
		{
			name:        "base with no path and absolute path",
			base:        "https://api.celestia.pops.one",
			path:        "/cosmos/bank/v1beta1/balances/someaddr",
			wantURL:     "https://api.celestia.pops.one/cosmos/bank/v1beta1/balances/someaddr",
			wantErr:     false,
			description: "host-only base gets request path appended",
		},
		{
			name:        "base with trailing slash",
			base:        "https://example.com/api/",
			path:        "/v1/query",
			wantURL:     "https://example.com/api/v1/query",
			wantErr:     false,
			description: "single slash between base path and request path",
		},
		{
			name:        "path with query string",
			base:        "https://gateway.example.com/rest/key",
			path:        "/cosmos/bank/v1beta1/balances/addr?height=100",
			wantURL:     "https://gateway.example.com/rest/key/cosmos/bank/v1beta1/balances/addr?height=100",
			wantErr:     false,
			description: "query string preserved",
		},
		{
			name:        "relative path is a path under the base path",
			base:        "https://example.com/base/",
			path:        "sub/foo",
			wantURL:     "https://example.com/base/sub/foo",
			wantErr:     false,
			description: "a path with no leading slash goes under the base path like one with",
		},
		{
			name:        "relative path never replaces the base path's last segment",
			base:        "https://example.com/base",
			path:        "sub/foo",
			wantURL:     "https://example.com/base/sub/foo",
			description: "ResolveReference would resolve it to /sub/foo and drop /base (the gateway key)",
		},
		{
			name:        "relative path with dot segments stays under the base path",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "../../admin",
			wantURL:     "https://node.vendor.example/gateway/KEY/admin",
			description: "resolved against the base path the dot segments would climb it",
		},
		{
			name:        "network-path reference stays a path on the configured host (MAG-3970)",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "//other.example",
			wantURL:     "https://node.vendor.example/gateway/KEY//other.example",
			description: "a leading // must not replace the host",
		},
		{
			name:        "network-path reference with a query stays on the configured host",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "//other.example?x=1",
			wantURL:     "https://node.vendor.example/gateway/KEY//other.example?x=1",
			description: "the query is kept, the host is not taken from the path",
		},
		{
			name:        "network-path reference with a port stays on the configured host",
			base:        "http://127.0.0.1:9101",
			path:        "//127.0.0.1:9200",
			wantURL:     "http://127.0.0.1:9101//127.0.0.1:9200",
			description: "host and port are the operator's",
		},
		{
			name:        "network-path reference with an IPv6 host stays on the configured host",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "//[::1]:9200",
			wantURL:     "https://node.vendor.example/gateway/KEY//%5B::1%5D:9200",
			description: "the brackets are path characters here, so String() escapes them",
		},
		{
			name:        "absolute URL as a path stays on the configured host",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "http://other.example",
			wantURL:     "https://node.vendor.example/gateway/KEY/http://other.example",
			description: "a scheme in the path must not change scheme or host, nor drop the base path",
		},
		{
			name:        "network-path reference with a path keeps the whole path",
			base:        "http://127.0.0.1:9101",
			path:        "//127.0.0.1:9200/x",
			wantURL:     "http://127.0.0.1:9101//127.0.0.1:9200/x",
			description: "the authority is part of the path; nothing is dropped before the node sees it",
		},
		{
			name:        "absolute URL with a path keeps the whole path",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "http://other.example/x",
			wantURL:     "https://node.vendor.example/gateway/KEY/http://other.example/x",
			description: "the scheme and authority are part of the path; nothing is dropped from either side",
		},
		{
			name:        "userinfo in the path is not taken as credentials for another host",
			base:        "https://node.vendor.example",
			path:        "//user:pass@other.example",
			wantURL:     "https://node.vendor.example//user:pass@other.example",
			description: "no host, no userinfo from the path",
		},
		{
			name:        "empty userinfo in the path is not taken as credentials for another host",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "//@other.example",
			wantURL:     "https://node.vendor.example/gateway/KEY//@other.example",
			description: "an empty userinfo still parses as an authority; it must stay a path",
		},
		{
			name:        "dot segments stay under the base path (MAG-5699)",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "/../../admin?x=1",
			wantURL:     "https://node.vendor.example/gateway/KEY/admin?x=1",
			description: "the path is resolved first, so the node gets a path under the base path",
		},
		{
			name:        "percent-encoded dot segments stay under the base path",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "/%2e%2e/%2E%2E/admin",
			wantURL:     "https://node.vendor.example/gateway/KEY/admin",
			description: "the path is decoded before the join, so an encoded dot segment is a dot segment",
		},
		{
			name:        "dot segments inside the path are resolved in place",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "/cosmos/../cosmos/bank/./v1beta1/params",
			wantURL:     "https://node.vendor.example/gateway/KEY/cosmos/bank/v1beta1/params",
			description: "only the dot segments change",
		},
		{
			name:        "a trailing dot segment keeps the trailing slash",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "/cosmos/bank/..",
			wantURL:     "https://node.vendor.example/gateway/KEY/cosmos/",
			description: "RFC 3986 resolution: /cosmos/bank/.. is /cosmos/",
		},
		{
			name:        "dot segments to the root are the base path",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "/cosmos/..",
			wantURL:     "https://node.vendor.example/gateway/KEY",
			description: "a path that resolves to / is the base path itself, as / is",
		},
		{
			name:        "empty segments and a trailing slash stay",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "/v2//info/",
			wantURL:     "https://node.vendor.example/gateway/KEY/v2//info/",
			description: "only dot segments are resolved; a spec can name an api with a trailing slash",
		},
		{
			name:        "dot segments after an authority stay under the base path",
			base:        "https://node.vendor.example/gateway/KEY",
			path:        "//other.example/../../../admin",
			wantURL:     "https://node.vendor.example/gateway/KEY/admin",
			description: "the authority is path text, and the dot segments after it are resolved like any",
		},
		{
			name:        "the base URL's query stays and the path's query follows it",
			base:        "https://gateway.example/v1?network=mainnet",
			path:        "/x?height=5",
			wantURL:     "https://gateway.example/v1/x?network=mainnet&height=5",
			description: "a node url can carry its key or its network in the query; the path's query must not replace it",
		},
		{
			name:        "the base URL's query stays when the path has none",
			base:        "https://gateway.example/v1?network=mainnet",
			path:        "/x",
			wantURL:     "https://gateway.example/v1/x?network=mainnet",
			description: "no query to append",
		},
		{
			name:        "a pair the base URL's query sets is left out of the path's query (MAG-5699)",
			base:        "https://gateway.example/v1?network=mainnet",
			path:        "/x?network=testnet&height=5",
			wantURL:     "https://gateway.example/v1/x?network=mainnet&height=5",
			description: "the base URL's parameters go as configured, first and only",
		},
		{
			name:        "a pair's name compares unescaped and lowercased",
			base:        "https://gateway.example/v1?network=mainnet",
			path:        "/x?Network=testnet&net%77ork=testnet&height=5",
			wantURL:     "https://gateway.example/v1/x?network=mainnet&height=5",
			description: "a server that reads names case-insensitively, or decoded, reads the same name",
		},
		{
			name:        "a pair ends at ; too, and the separators it keeps stay as they were",
			base:        "https://gateway.example/v1?network=mainnet",
			path:        "/x?a=1;network=testnet;b=2",
			wantURL:     "https://gateway.example/v1/x?network=mainnet&a=1;b=2",
			description: "some servers split on ; as well as &",
		},
		{
			name:        "a path query that only repeats the base URL's pairs adds nothing",
			base:        "https://gateway.example/v1?network=mainnet",
			path:        "/x?network=testnet",
			wantURL:     "https://gateway.example/v1/x?network=mainnet",
			description: "no dangling separator",
		},
		{
			name:        "invalid base URL",
			base:        "://invalid",
			path:        "/path",
			wantURL:     "",
			wantErr:     true,
			description: "invalid base returns error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JoinURLPath(tt.base, tt.path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, got, tt.description)
		})
	}
}

// TestNodeUrlRequestURL pins RequestURL: JoinURLPath on the node url, and the auth-query's names are
// reserved too, so AddAuthPath appends the operator's value as the only one (MAG-5699).
func TestNodeUrlRequestURL(t *testing.T) {
	nodeURL := &NodeUrl{
		Url:        "https://gateway.example/v1?network=mainnet",
		AuthConfig: AuthConfig{AuthQuery: "apikey=OPERATOR;tenant=ops"},
	}
	for _, tt := range []struct {
		name        string
		path        string
		wantURL     string
		wantSent    string
		description string
	}{
		{
			name:        "a path with a query of its own",
			path:        "/cosmos/bank/v1beta1/params?height=5",
			wantURL:     "https://gateway.example/v1/cosmos/bank/v1beta1/params?network=mainnet&height=5",
			wantSent:    "https://gateway.example/v1/cosmos/bank/v1beta1/params?network=mainnet&height=5&apikey=OPERATOR;tenant=ops",
			description: "the node url's query, the path's, then the auth-query",
		},
		{
			name:        "a pair the auth-query sets is left out",
			path:        "/cosmos/bank/v1beta1/params?apikey=THEIRS&Tenant=theirs&height=5",
			wantURL:     "https://gateway.example/v1/cosmos/bank/v1beta1/params?network=mainnet&height=5",
			wantSent:    "https://gateway.example/v1/cosmos/bank/v1beta1/params?network=mainnet&height=5&apikey=OPERATOR;tenant=ops",
			description: "the auth-query's value is the only one sent, so a server reads it first or last alike",
		},
		{
			name:        "a pair the node url's query sets is left out",
			path:        "/cosmos/bank/v1beta1/params?network=testnet",
			wantURL:     "https://gateway.example/v1/cosmos/bank/v1beta1/params?network=mainnet",
			wantSent:    "https://gateway.example/v1/cosmos/bank/v1beta1/params?network=mainnet&apikey=OPERATOR;tenant=ops",
			description: "as JoinURLPath leaves it out",
		},
		{
			name:        "dot segments stay under the node url's path",
			path:        "/../../admin?apikey=THEIRS",
			wantURL:     "https://gateway.example/v1/admin?network=mainnet",
			wantSent:    "https://gateway.example/v1/admin?network=mainnet&apikey=OPERATOR;tenant=ops",
			description: "as JoinURLPath resolves them",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := nodeURL.RequestURL(tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, got, tt.description)
			assert.Equal(t, tt.wantSent, nodeURL.AuthConfig.AddAuthPath(got), "with the auth-query added as the sender adds it")
		})
	}

	t.Run("no auth-query reserves nothing", func(t *testing.T) {
		plain := &NodeUrl{Url: "https://gateway.example/v1"}
		got, err := plain.RequestURL("/x?apikey=THEIRS")
		require.NoError(t, err)
		assert.Equal(t, "https://gateway.example/v1/x?apikey=THEIRS", got)
	})

	t.Run("a nil node url is an error, not a panic", func(t *testing.T) {
		var none *NodeUrl
		_, err := none.RequestURL("/x")
		require.Error(t, err)
	})
}
