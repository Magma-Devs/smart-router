package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// httpOnlyCollections is the probe for a spec whose every internal path is an
// http collection — the shape of all six internal-path families but STRK.
func httpOnlyCollections(string, []string) bool { return false }

// The direct-RPC endpoint list has to name the same urls the chain router's
// per-path proxies do, because both answer the same api collections — one for
// the tracker and the verifications, the other for relays. They diverged once,
// and TON answered v3 apis out of the v2 upstream.
func TestExpandInternalPaths(t *testing.T) {
	t.Run("a spec with no internal paths is left alone", func(t *testing.T) {
		urls := []common.NodeUrl{{Url: "https://eth.example"}}
		require.Equal(t, urls, expandInternalPaths(urls, []string{""}, httpOnlyCollections))
		require.Equal(t, urls, expandInternalPaths(urls, nil, httpOnlyCollections))
	})

	t.Run("a shared root yields one url per path, with the path appended", func(t *testing.T) {
		// chainstack serves both TON versions under one base, exactly like the
		// chain router's autoGenerateMissingInternalPaths.
		got := expandInternalPaths(
			[]common.NodeUrl{{Url: "https://ton.example/api"}},
			[]string{"/v3", "", "/v2"},
			httpOnlyCollections,
		)
		require.Equal(t, []common.NodeUrl{
			{Url: "https://ton.example/api"},
			{Url: "https://ton.example/api/v2", InternalPath: "/v2"},
			{Url: "https://ton.example/api/v3", InternalPath: "/v3"},
		}, got)
	})

	t.Run("a pinned url is taken as it stands", func(t *testing.T) {
		// tatum serves TON v2 at the host root and v3 under /api/v3, each
		// pinned. Appending the path again would ask for /v2/v2/…
		urls := []common.NodeUrl{
			{Url: "https://ton.tatum.example", InternalPath: "/v2"},
			{Url: "https://ton.tatum.example/api/v3", InternalPath: "/v3"},
		}
		require.Equal(t, urls, expandInternalPaths(urls, []string{"", "/v2", "/v3"}, httpOnlyCollections))
	})

	t.Run("a mixed provider expands only the unpinned urls", func(t *testing.T) {
		got := expandInternalPaths(
			[]common.NodeUrl{
				{Url: "https://ton.example/api"},
				{Url: "https://ton.tatum.example", InternalPath: "/v2"},
			},
			[]string{"", "/v2", "/v3"},
			httpOnlyCollections,
		)
		require.Equal(t, []common.NodeUrl{
			{Url: "https://ton.example/api"},
			{Url: "https://ton.example/api/v2", InternalPath: "/v2"},
			{Url: "https://ton.example/api/v3", InternalPath: "/v3"},
			{Url: "https://ton.tatum.example", InternalPath: "/v2"},
		}, got)
	})

	t.Run("expansion carries the url's addons", func(t *testing.T) {
		// The generated urls are the same upstream, so they serve whatever it
		// serves — dropping the addons would make the archive leg unreachable
		// on every internal path but the root.
		got := expandInternalPaths(
			[]common.NodeUrl{{Url: "https://avax.example", Addons: []string{"archive"}}},
			[]string{"", "/P"},
			httpOnlyCollections,
		)
		require.Len(t, got, 2)
		require.Equal(t, []string{"archive"}, got[1].Addons)
		require.Equal(t, "/P", got[1].InternalPath)
	})

	t.Run("a collection LABEL is not appended to a url", func(t *testing.T) {
		// STRK carries its shared api set in "HTTP-ONLY" / "WS-ONLY"
		// collections that exist only to be inherited. They are disabled, so
		// the parser never reports them — but the idiom is in the spec repo,
		// and an enabled one must not produce `https://host` + `HTTP-ONLY`.
		got := expandInternalPaths(
			[]common.NodeUrl{{Url: "https://strk.example"}},
			[]string{"", "HTTP-ONLY", "WS-ONLY", "/rpc/v0_9"},
			httpOnlyCollections,
		)
		require.Equal(t, []common.NodeUrl{
			{Url: "https://strk.example"},
			{Url: "https://strk.example/rpc/v0_9", InternalPath: "/rpc/v0_9"},
		}, got)
	})

	t.Run("a pinned url equal to base+path is not emitted twice", func(t *testing.T) {
		// Legitimate config, and the generated twin would just be probed and
		// registered in the metrics a second time.
		got := expandInternalPaths(
			[]common.NodeUrl{
				{Url: "https://ton.example/api"},
				{Url: "https://ton.example/api/v2", InternalPath: "/v2"},
			},
			[]string{"", "/v2", "/v3"},
			httpOnlyCollections,
		)
		require.Equal(t, []common.NodeUrl{
			{Url: "https://ton.example/api"},
			{Url: "https://ton.example/api/v2", InternalPath: "/v2"},
			{Url: "https://ton.example/api/v3", InternalPath: "/v3"},
		}, got)
	})

	t.Run("a url that already ends in the path is that path's endpoint, not a parent", func(t *testing.T) {
		// An operator who baked the version into the url instead of declaring
		// `internal-path` — a config that works today, because before the
		// endpoint list carried paths at all every relay went to that one url.
		// Appending would ask the vendor for /rpc/v0_8/rpc/v0_8.
		got := expandInternalPaths(
			[]common.NodeUrl{{Url: "https://vendor.example/rpc/v0_8"}},
			[]string{"", "/rpc/v0_8", "/rpc/v0_9"},
			httpOnlyCollections,
		)
		require.Equal(t, []common.NodeUrl{
			{Url: "https://vendor.example/rpc/v0_8"},
			{Url: "https://vendor.example/rpc/v0_8", InternalPath: "/rpc/v0_8"},
			{Url: "https://vendor.example/rpc/v0_8/rpc/v0_9", InternalPath: "/rpc/v0_9"},
		}, got)
	})

	t.Run("a path is generated only on the transport that serves it", func(t *testing.T) {
		// STRK's real enabled path set. Its `/ws/rpc/v0_x` collections inherit
		// WS-ONLY, which carries SUBSCRIBE; the `/rpc/v0_x` ones inherit
		// HTTP-ONLY, which does not. A config carrying both schemes must yield
		// the http paths on https and the ws paths on wss, and neither the
		// other way round — `https://host/ws/rpc/v0_8` and
		// `wss://host/rpc/v0_9` are urls chain_router.go refuses to build and
		// no upstream answers.
		strkPaths := []string{
			"", "/rpc/v0_8", "/rpc/v0_9", "/rpc/v0_10", "/rpc/pathfinder/v0.1",
			"/ws/rpc/v0_8", "/ws/rpc/v0_9", "/ws/rpc/v0_10",
		}
		subscriptionCollections := func(internalPath string, _ []string) bool {
			return strings.HasPrefix(internalPath, "/ws/")
		}

		got := expandInternalPaths(
			[]common.NodeUrl{
				{Url: "https://strk.example"},
				{Url: "wss://strk.example"},
			},
			strkPaths,
			subscriptionCollections,
		)

		// Nine urls — the same nine chain_router.go builds proxies for.
		require.Equal(t, []common.NodeUrl{
			{Url: "https://strk.example"},
			{Url: "https://strk.example/rpc/pathfinder/v0.1", InternalPath: "/rpc/pathfinder/v0.1"},
			{Url: "https://strk.example/rpc/v0_10", InternalPath: "/rpc/v0_10"},
			{Url: "https://strk.example/rpc/v0_8", InternalPath: "/rpc/v0_8"},
			{Url: "https://strk.example/rpc/v0_9", InternalPath: "/rpc/v0_9"},
			{Url: "wss://strk.example"},
			{Url: "wss://strk.example/ws/rpc/v0_10", InternalPath: "/ws/rpc/v0_10"},
			{Url: "wss://strk.example/ws/rpc/v0_8", InternalPath: "/ws/rpc/v0_8"},
			{Url: "wss://strk.example/ws/rpc/v0_9", InternalPath: "/ws/rpc/v0_9"},
		}, got)
	})

	t.Run("a url with no parseable scheme expands its http paths", func(t *testing.T) {
		// gRPC node-urls are a bare host:port. They are not ws, so they carry
		// the spec's non-subscription collections like any http url does.
		got := expandInternalPaths(
			[]common.NodeUrl{{Url: "avax.example:9090"}},
			[]string{"", "/P"},
			httpOnlyCollections,
		)
		require.Equal(t, []common.NodeUrl{
			{Url: "avax.example:9090"},
			{Url: "avax.example:9090/P", InternalPath: "/P"},
		}, got)
	})

	t.Run("output order is stable whatever order the paths arrive in", func(t *testing.T) {
		urls := []common.NodeUrl{{Url: "https://ton.example/api"}}
		first := expandInternalPaths(urls, []string{"/v3", "/v2", ""}, httpOnlyCollections)
		second := expandInternalPaths(urls, []string{"", "/v2", "/v3"}, httpOnlyCollections)
		require.Equal(t, first, second)
	})
}
