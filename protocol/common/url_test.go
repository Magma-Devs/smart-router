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
			base:        "https://g.w.lavanet.xyz:443/gateway/lava/rest/YOUR_LAVA_GATEWAY_KEY",
			path:        "/cosmos/bank/v1beta1/balances/lava@1ax3pa3eg0k5wnmuj8v3k2yd87a4ka5g9ymwdu3",
			wantURL:     "https://g.w.lavanet.xyz:443/gateway/lava/rest/YOUR_LAVA_GATEWAY_KEY/cosmos/bank/v1beta1/balances/lava@1ax3pa3eg0k5wnmuj8v3k2yd87a4ka5g9ymwdu3",
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
			name:        "relative path uses ResolveReference",
			base:        "https://example.com/base/",
			path:        "sub/foo",
			wantURL:     "https://example.com/base/sub/foo",
			wantErr:     false,
			description: "relative path resolved against base",
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
