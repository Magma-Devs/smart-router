package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// The direct-RPC endpoint list has to name the same urls the chain router's
// per-path proxies do, because both answer the same api collections — one for
// the tracker and the verifications, the other for relays. They diverged once,
// and TON answered v3 apis out of the v2 upstream.
func TestExpandInternalPaths(t *testing.T) {
	t.Run("a spec with no internal paths is left alone", func(t *testing.T) {
		urls := []common.NodeUrl{{Url: "https://eth.example"}}
		require.Equal(t, urls, expandInternalPaths(urls, []string{""}))
		require.Equal(t, urls, expandInternalPaths(urls, nil))
	})

	t.Run("a shared root yields one url per path, with the path appended", func(t *testing.T) {
		// chainstack serves both TON versions under one base, exactly like the
		// chain router's autoGenerateMissingInternalPaths.
		got := expandInternalPaths(
			[]common.NodeUrl{{Url: "https://ton.example/api"}},
			[]string{"/v3", "", "/v2"},
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
		require.Equal(t, urls, expandInternalPaths(urls, []string{"", "/v2", "/v3"}))
	})

	t.Run("a mixed provider expands only the unpinned urls", func(t *testing.T) {
		got := expandInternalPaths(
			[]common.NodeUrl{
				{Url: "https://ton.example/api"},
				{Url: "https://ton.tatum.example", InternalPath: "/v2"},
			},
			[]string{"", "/v2", "/v3"},
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
		)
		require.Equal(t, []common.NodeUrl{
			{Url: "https://ton.example/api"},
			{Url: "https://ton.example/api/v2", InternalPath: "/v2"},
			{Url: "https://ton.example/api/v3", InternalPath: "/v3"},
		}, got)
	})

	t.Run("output order is stable whatever order the paths arrive in", func(t *testing.T) {
		urls := []common.NodeUrl{{Url: "https://ton.example/api"}}
		first := expandInternalPaths(urls, []string{"/v3", "/v2", ""})
		second := expandInternalPaths(urls, []string{"", "/v2", "/v3"})
		require.Equal(t, first, second)
	})
}
