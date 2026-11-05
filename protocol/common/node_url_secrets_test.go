package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-4863: every place a node-url config carries a secret, and the form it is registered in.
func TestNodeUrlSecretValues(t *testing.T) {
	cases := []struct {
		name                      string
		nodeUrl                   NodeUrl
		wantCredentials, wantCand []string
	}{
		{
			name:     "a key in the path",
			nodeUrl:  NodeUrl{Url: "https://eth-mainnet.example.com/v2/KEY123SECRET"},
			wantCand: []string{"v2", "KEY123SECRET"},
		},
		{
			name:     "a key in the query, after a ;",
			nodeUrl:  NodeUrl{Url: "https://eth.example.com/rpc?a=1;apikey=KEY123SECRET"},
			wantCand: []string{"rpc", "1", "KEY123SECRET"},
		},
		{
			name:     "a percent-encoded query value is registered unescaped",
			nodeUrl:  NodeUrl{Url: "https://eth.example.com/?apikey=KEY%2B123%2FSECRET"},
			wantCand: []string{"KEY+123/SECRET"},
		},
		{
			name:            "a password",
			nodeUrl:         NodeUrl{Url: "https://user:KEY123SECRET@eth.example.com/"},
			wantCredentials: []string{"KEY123SECRET"},
			wantCand:        []string{"user"},
		},
		{
			name:            "a key standing alone as the user",
			nodeUrl:         NodeUrl{Url: "wss://KEY123SECRET@eth.example.com/ws"},
			wantCredentials: []string{"KEY123SECRET"},
			wantCand:        []string{"ws"},
		},
		{
			name:     "a scheme-less address with a dotless host",
			nodeUrl:  NodeUrl{Url: "node-host/v2/KEY123SECRET"},
			wantCand: []string{"v2", "KEY123SECRET"},
		},
		{
			name: "auth-query and auth-headers",
			nodeUrl: NodeUrl{Url: "https://eth.example.com", AuthConfig: AuthConfig{
				AuthQuery:   "apikey=QUERY123SECRET;other=x",
				AuthHeaders: map[string]string{"Authorization": "Bearer TOKEN123SECRET"},
			}},
			wantCredentials: []string{"QUERY123SECRET", "x", "Bearer TOKEN123SECRET", "Bearer", "TOKEN123SECRET"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			credentials, candidates := tc.nodeUrl.SecretValues()
			for _, want := range tc.wantCredentials {
				require.Contains(t, credentials, want)
			}
			for _, want := range tc.wantCand {
				require.Contains(t, candidates, want)
			}
		})
	}
}

// A nil node-url, or one with no url, holds nothing to register.
func TestNodeUrlSecretValues_Empty(t *testing.T) {
	var none *NodeUrl
	credentials, candidates := none.SecretValues()
	require.Empty(t, credentials)
	require.Empty(t, candidates)
	credentials, candidates = (&NodeUrl{}).SecretValues()
	require.Empty(t, credentials)
	require.Empty(t, candidates)
}
