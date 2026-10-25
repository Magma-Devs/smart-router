package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

func splitTestProvider(name string, urls ...common.NodeUrl) *lavasession.RPCStaticProviderEndpoint {
	return &lavasession.RPCStaticProviderEndpoint{
		ChainID:      "TONT",
		ApiInterface: "rest",
		Name:         name,
		NodeUrls:     urls,
		Stake:        700,
		GroupLabel:   "tier-1",
	}
}

func entityNames(entities []*lavasession.RPCStaticProviderEndpoint) []string {
	names := make([]string, 0, len(entities))
	for _, entity := range entities {
		names = append(names, entity.Name)
	}
	return names
}

// MAG-4219. The router keys every mechanism by provider name, so two urls under one provider
// shared one score, one blocked slot, one retry unit, one health gauge and one latest-block
// series, and the first usable url in config order served every relay. The split gives each url
// its own provider entity before anything downstream sees it.
func TestSplitProvidersPerURL(t *testing.T) {
	noPaths := []string{""}

	t.Run("one url is name/0, with the parent's fields", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack", common.NodeUrl{Url: "https://a.example"}),
		}, noPaths, httpOnlyCollections)
		require.Len(t, got, 1)
		require.Equal(t, "chainstack/0", got[0].Name)
		require.Equal(t, "chainstack", got[0].Parent)
		require.Equal(t, []common.NodeUrl{{Url: "https://a.example"}}, got[0].NodeUrls)
		require.Equal(t, "TONT", got[0].ChainID)
		require.Equal(t, "rest", got[0].ApiInterface)
		require.Equal(t, int64(700), got[0].Stake)
		require.Equal(t, "tier-1", got[0].GroupLabel)
	})

	t.Run("three urls are three entities in config order", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://a.example"},
				common.NodeUrl{Url: "https://b.example", Addons: []string{"archive"}},
				common.NodeUrl{Url: "wss://c.example"},
			),
			splitTestProvider("tatum", common.NodeUrl{Url: "https://t.example"}),
		}, noPaths, httpOnlyCollections)
		require.Equal(t, []string{"chainstack/0", "chainstack/1", "chainstack/2", "tatum/0"}, entityNames(got))
		for _, entity := range got {
			require.Len(t, entity.NodeUrls, 1)
		}
		require.Equal(t, []string{"archive"}, got[1].NodeUrls[0].Addons)
		require.Equal(t, "tatum", got[3].Parent)
	})

	t.Run("the configured provider is not modified", func(t *testing.T) {
		provider := splitTestProvider("chainstack",
			common.NodeUrl{Url: "https://a.example"},
			common.NodeUrl{Url: "https://b.example"},
		)
		splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{provider}, noPaths, httpOnlyCollections)
		require.Equal(t, "chainstack", provider.Name)
		require.Empty(t, provider.Parent)
		require.Len(t, provider.NodeUrls, 2)
	})

	t.Run("a shared root yields the root entity and one entity per path", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://ton.example/api"},
				common.NodeUrl{Url: "https://other.example/v3", InternalPath: "/v3"},
			),
		}, []string{"", "/v2", "/v3"}, httpOnlyCollections)
		require.Equal(t, []string{"chainstack/0", "chainstack/0/v2", "chainstack/0/v3", "chainstack/1"}, entityNames(got))
		require.Equal(t, common.NodeUrl{Url: "https://ton.example/api"}, got[0].NodeUrls[0])
		require.Equal(t, common.NodeUrl{Url: "https://ton.example/api/v2", InternalPath: "/v2"}, got[1].NodeUrls[0])
		require.Equal(t, common.NodeUrl{Url: "https://ton.example/api/v3", InternalPath: "/v3"}, got[2].NodeUrls[0])
		// A url declaring its own internal-path is a configured url: plain index, not expanded.
		require.Equal(t, common.NodeUrl{Url: "https://other.example/v3", InternalPath: "/v3"}, got[3].NodeUrls[0])
		for _, entity := range got {
			require.Equal(t, "chainstack", entity.Parent)
		}
	})

	t.Run("the index is the configured position, so a later url keeps its name when an earlier one expands", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://ton.example/api"},
				common.NodeUrl{Url: "wss://ton.example/ws", InternalPath: "/ws"},
			),
		}, []string{"", "/v2"}, httpOnlyCollections)
		require.Equal(t, []string{"chainstack/0", "chainstack/0/v2", "chainstack/1"}, entityNames(got))
	})

	t.Run("the same node listed twice is one entity with both add-on sets", func(t *testing.T) {
		auth := common.AuthConfig{AuthHeaders: map[string]string{"x-api-key": "k1"}}
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://a.example", AuthConfig: auth, Addons: []string{"archive"}, StandaloneAddons: true, Methods: []string{"eth_getLogs"}},
				common.NodeUrl{Url: "https://b.example"},
				common.NodeUrl{Url: "https://a.example", AuthConfig: auth, Addons: []string{"debug", "archive"}, SkipVerifications: []string{"pruning"}},
			),
		}, noPaths, httpOnlyCollections)
		require.Equal(t, []string{"chainstack/0", "chainstack/1"}, entityNames(got))
		merged := got[0].NodeUrls[0]
		require.Equal(t, []string{"archive", "debug"}, merged.Addons)
		require.Equal(t, []string{"pruning"}, merged.SkipVerifications)
		require.Equal(t, []string{"eth_getLogs"}, merged.Methods)
		require.False(t, merged.StandaloneAddons, "an entry that did not opt out claims the base collection for the node")
		require.Equal(t, auth, merged.AuthConfig)
	})

	t.Run("the same url with different credentials is two nodes", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://a.example", AuthConfig: common.AuthConfig{AuthHeaders: map[string]string{"x-api-key": "archive-key"}}, Addons: []string{"archive"}},
				common.NodeUrl{Url: "https://a.example", AuthConfig: common.AuthConfig{AuthHeaders: map[string]string{"x-api-key": "debug-key"}}, Addons: []string{"debug"}},
				common.NodeUrl{Url: "https://a.example", AuthConfig: common.AuthConfig{AuthQuery: "key=q"}},
				common.NodeUrl{Url: "https://a.example", AuthConfig: common.AuthConfig{UseTLS: true}},
			),
		}, noPaths, httpOnlyCollections)
		require.Equal(t, []string{"chainstack/0", "chainstack/1", "chainstack/2", "chainstack/3"}, entityNames(got))
		require.Equal(t, []string{"archive"}, got[0].NodeUrls[0].Addons)
		require.Equal(t, []string{"debug"}, got[1].NodeUrls[0].Addons)
	})

	t.Run("the same url on different internal paths is two nodes", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://a.example/x", InternalPath: "/v2"},
				common.NodeUrl{Url: "https://a.example/x", InternalPath: "/v3"},
			),
		}, []string{"", "/v2", "/v3"}, httpOnlyCollections)
		require.Equal(t, []string{"chainstack/0", "chainstack/1"}, entityNames(got))
	})

	t.Run("a merged node keeps the first entry's connection settings", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://a.example", Timeout: 3 * time.Second, Addons: []string{"archive"}},
				common.NodeUrl{Url: "https://a.example", Timeout: 9 * time.Second, IpForwarding: true, Addons: []string{"debug"}},
			),
		}, noPaths, httpOnlyCollections)
		require.Len(t, got, 1)
		require.Equal(t, 3*time.Second, got[0].NodeUrls[0].Timeout)
		require.False(t, got[0].NodeUrls[0].IpForwarding)
		require.Equal(t, []string{"archive", "debug"}, got[0].NodeUrls[0].Addons)
	})

	t.Run("a merged node is expanded once", func(t *testing.T) {
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack",
				common.NodeUrl{Url: "https://ton.example/api", Addons: []string{"archive"}},
				common.NodeUrl{Url: "https://ton.example/api", Addons: []string{"debug"}},
			),
		}, []string{"", "/v2"}, httpOnlyCollections)
		require.Equal(t, []string{"chainstack/0", "chainstack/0/v2"}, entityNames(got))
		require.Equal(t, []string{"archive", "debug"}, got[1].NodeUrls[0].Addons)
	})

	t.Run("deterministic", func(t *testing.T) {
		providers := []*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("b", common.NodeUrl{Url: "https://b1.example"}, common.NodeUrl{Url: "https://b2.example"}),
			splitTestProvider("a", common.NodeUrl{Url: "https://a.example/api"}),
		}
		first := splitProvidersPerURL(providers, []string{"", "/v3", "/v2"}, httpOnlyCollections)
		second := splitProvidersPerURL(providers, []string{"", "/v3", "/v2"}, httpOnlyCollections)
		require.Equal(t, entityNames(first), entityNames(second))
		require.Equal(t, []string{"b/0", "b/0/v2", "b/0/v3", "b/1", "b/1/v2", "b/1/v3", "a/0", "a/0/v2", "a/0/v3"}, entityNames(first))
	})

	t.Run("empty and nil input", func(t *testing.T) {
		require.Empty(t, splitProvidersPerURL(nil, noPaths, httpOnlyCollections))
		require.Empty(t, splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{nil}, noPaths, httpOnlyCollections))
	})

	t.Run("entity names are checked for collisions like configured names", func(t *testing.T) {
		// A configured name may spell an entity name of another provider: "chainstack/0" with a
		// second url is "chainstack/0/1", which is also what a shared root of "chainstack" yields
		// on a spec whose internal path is "/1". Far-fetched, which is why the guard is the
		// existing validator and not a rule on names.
		got := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{
			splitTestProvider("chainstack", common.NodeUrl{Url: "https://a.example"}),
			splitTestProvider("chainstack/0", common.NodeUrl{Url: "https://b.example"}, common.NodeUrl{Url: "https://c.example"}),
		}, []string{"", "/1"}, httpOnlyCollections)
		require.Equal(t, []string{
			"chainstack/0", "chainstack/0/1",
			"chainstack/0/0", "chainstack/0/0/1", "chainstack/0/1", "chainstack/0/1/1",
		}, entityNames(got))
		require.Error(t, lavasession.ValidateUniqueProviderNames(got))
	})
}

// The helpers downstream of the split read entities the way they read providers: a group label
// is inherited by every url, and the url-to-name map used by relay_end names the entity.
func TestSplitProvidersPerURL_DownstreamHelpersSeeEntities(t *testing.T) {
	chainstack := splitTestProvider("chainstack",
		common.NodeUrl{Url: "https://a.example"},
		common.NodeUrl{Url: "https://b.example"},
	)
	tatum := splitTestProvider("tatum", common.NodeUrl{Url: "https://t.example"})
	tatum.GroupLabel = ""
	entities := splitProvidersPerURL([]*lavasession.RPCStaticProviderEndpoint{chainstack, tatum}, []string{""}, httpOnlyCollections)

	groups := staticProviderGroupAssignments(entities)
	require.Equal(t, map[string][]string{
		"tier-1":                    {"chainstack/0", "chainstack/1"},
		common.DefaultProviderGroup: {"tatum/0"},
	}, groups)

	names := providerNamesByURL(entities)
	require.Equal(t, map[string]string{
		"https://a.example": "chainstack/0",
		"https://b.example": "chainstack/1",
		"https://t.example": "tatum/0",
	}, names)

	require.NoError(t, lavasession.ValidateUniqueProviderNames(entities))
}
