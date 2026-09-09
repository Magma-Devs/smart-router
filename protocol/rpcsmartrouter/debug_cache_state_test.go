package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// stubCacheStateBackend is a CacheBackend that reports a canned DebugCacheState, so
// every tier shape can be exercised without standing up a gRPC client or a Redis.
type stubCacheStateBackend struct {
	state performance.DebugCacheState
	// probed records that something asked this backend for liveness through the
	// SERVING path. The endpoint must never do that: on the real gRPC client
	// CacheActive() reaches getClient(), which spawns a reconnect.
	probed *bool
}

func (s *stubCacheStateBackend) CacheActive() bool {
	if s.probed != nil {
		*s.probed = true
	}
	return true
}

func (s *stubCacheStateBackend) GetEntry(ctx context.Context, _ *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	return &pairingtypes.CacheRelayReply{}, nil
}

func (s *stubCacheStateBackend) SetEntry(ctx context.Context, _ *pairingtypes.RelayCacheSet) error {
	return nil
}
func (s *stubCacheStateBackend) Flush(ctx context.Context) error { return nil }
func (s *stubCacheStateBackend) Close() error                    { return nil }
func (s *stubCacheStateBackend) DebugCacheState() performance.DebugCacheState {
	return s.state
}

// stubCacheStateReader is the secondary-tier equivalent (CacheReader is read-only).
type stubCacheStateReader struct {
	state performance.DebugCacheState
}

func (s *stubCacheStateReader) CacheActive() bool { return true }
func (s *stubCacheStateReader) GetEntry(ctx context.Context, _ *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	return &pairingtypes.CacheRelayReply{}, nil
}
func (s *stubCacheStateReader) DebugCacheState() performance.DebugCacheState { return s.state }

func getCacheState(t *testing.T, deps debugMuxDeps) (*httptest.ResponseRecorder, debugCacheStateResponse) {
	t.Helper()
	mux := buildDebugMux(deps)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/debug/cache-state", nil))

	var resp debugCacheStateResponse
	if rr.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp), "body: %s", rr.Body.String())
	}
	return rr, resp
}

func grpcTier(address string, reachable *bool) performance.DebugCacheState {
	return performance.DebugCacheState{
		Configured:      true,
		Engine:          performance.CacheEngineGRPC,
		Address:         address,
		Reachable:       reachable,
		WhenUnreachable: performance.CacheWhenUnreachableSkipped,
	}
}

func boolPtr(b bool) *bool { return &b }

// The regression that made this endpoint lie on the DEFAULT deployment: a router
// started with no --cache-be at all reported that it had a gRPC cache.
//
// SelectCacheBackend returns `var cache CacheBackend = (*Cache)(nil)` when nothing is
// configured. A typed nil inside a non-nil interface still satisfies
// DebugCacheStateReporter, so a handler that decided "configured" by whether the
// assertion succeeded took the branch and answered engine "grpc", configured true —
// byte-identical to a configured-but-down cache except for a missing address.
func TestCacheStateTypedNilCacheIsNotConfigured(t *testing.T) {
	t.Run("typed-nil primary reports no cache", func(t *testing.T) {
		var unconfigured performance.CacheBackend = (*performance.Cache)(nil)

		_, resp := getCacheState(t, debugMuxDeps{cache: unconfigured})

		require.Equal(t, "none", resp.Engine,
			"a router with no cache configured must not claim an engine")
		require.False(t, resp.Tiers.Primary.Configured)
		require.Nil(t, resp.Tiers.Primary.Reachable,
			"an absent tier has no reachability to report")
		require.Empty(t, resp.Tiers.Primary.Engine,
			"the engine of the type carrying the nil is not a fact about this router")
	})

	t.Run("typed-nil secondary reports no cache", func(t *testing.T) {
		var unconfigured performance.CacheReader = (*performance.Cache)(nil)

		_, resp := getCacheState(t, debugMuxDeps{secondaryCache: unconfigured})

		require.Equal(t, "none", resp.Engine)
		require.False(t, resp.Tiers.Secondary.Configured)
	})

	t.Run("a nil interface is equally safe", func(t *testing.T) {
		_, resp := getCacheState(t, debugMuxDeps{})
		require.Equal(t, "none", resp.Engine)
		require.False(t, resp.Tiers.Primary.Configured)
		require.False(t, resp.Tiers.Secondary.Configured)
	})
}

// engine is a convenience field read from the tier that is serving. A secondary-only
// router is a documented topology — reads work, nothing backfills — so reporting
// "none" there would tell a reader no cache is in play while one serves every read.
func TestCacheStateEngineFallsBackToSecondary(t *testing.T) {
	t.Run("secondary-only router names the secondary's engine", func(t *testing.T) {
		_, resp := getCacheState(t, debugMuxDeps{
			secondaryCache: &stubCacheStateReader{state: grpcTier("secondary:20100", boolPtr(true))},
		})

		require.Equal(t, performance.CacheEngineGRPC, resp.Engine,
			"a configured secondary is an engine in use")
		require.False(t, resp.Tiers.Primary.Configured)
		require.True(t, resp.Tiers.Secondary.Configured)
	})

	t.Run("primary wins when both are configured", func(t *testing.T) {
		_, resp := getCacheState(t, debugMuxDeps{
			cache: &stubCacheStateBackend{state: performance.DebugCacheState{
				Configured: true, Engine: performance.CacheEngineRESP, Address: "redis:6379",
				WhenUnreachable: performance.CacheWhenUnreachableAttempted,
			}},
			secondaryCache: &stubCacheStateReader{state: grpcTier("secondary:20100", boolPtr(true))},
		})

		require.Equal(t, performance.CacheEngineRESP, resp.Engine)
	})

	// The pairing docs call unsupported and nothing validates: the secondary is
	// always a gRPC client while the primary becomes RESP whenever the resp-cache
	// block is set. A single top-level engine cannot express it, which is why the
	// tier carries its own — this is the endpoint's most valuable diagnosis.
	t.Run("a mixed-engine deployment is visible per tier", func(t *testing.T) {
		_, resp := getCacheState(t, debugMuxDeps{
			cache: &stubCacheStateBackend{state: performance.DebugCacheState{
				Configured: true, Engine: performance.CacheEngineRESP, Address: "redis:6379",
				WhenUnreachable: performance.CacheWhenUnreachableAttempted,
			}},
			secondaryCache: &stubCacheStateReader{state: grpcTier("cache-be:20100", boolPtr(true))},
		})

		require.Equal(t, performance.CacheEngineRESP, resp.Tiers.Primary.Engine)
		require.Equal(t, performance.CacheEngineGRPC, resp.Tiers.Secondary.Engine,
			"the two tiers running different engines must be readable, not averaged away")
	})
}

// reachable:false means opposite things per engine — the gRPC tier is skipped before
// any I/O, the RESP tier still issues every lookup and pays the full timeout. Same
// key, opposite operational cost, so the payload has to say which.
func TestCacheStateReportsWhatUnreachableCosts(t *testing.T) {
	_, resp := getCacheState(t, debugMuxDeps{
		cache: &stubCacheStateBackend{state: performance.DebugCacheState{
			Configured: true, Engine: performance.CacheEngineRESP, Address: "redis:6379",
			Reachable:       boolPtr(false),
			WhenUnreachable: performance.CacheWhenUnreachableAttempted,
		}},
		secondaryCache: &stubCacheStateReader{state: grpcTier("cache-be:20100", boolPtr(false))},
	})

	require.Equal(t, performance.CacheWhenUnreachableAttempted, resp.Tiers.Primary.WhenUnreachable,
		"a dead RESP backend is still asked on every relay")
	require.Equal(t, performance.CacheWhenUnreachableSkipped, resp.Tiers.Secondary.WhenUnreachable,
		"a dead gRPC tier is bypassed and costs nothing")
}

// null reachability is a real third state, not a missing value: a RESP backend before
// its first probe returns, a gRPC connection mid-dial. Collapsing it into false
// reports a healthy backend as down to anything that polls at startup.
func TestCacheStateReachabilityIsThreeState(t *testing.T) {
	cases := []struct {
		name      string
		reachable *bool
		wantJSON  string
	}{
		{"reachable", boolPtr(true), `"reachable":true`},
		{"unreachable", boolPtr(false), `"reachable":false`},
		{"not determined yet", nil, `"reachable":null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr, resp := getCacheState(t, debugMuxDeps{
				cache: &stubCacheStateBackend{state: grpcTier("cache-be:20100", tc.reachable)},
			})

			require.Contains(t, rr.Body.String(), tc.wantJSON)
			require.True(t, resp.Tiers.Primary.Configured,
				"reachability never decides whether a tier is configured")
			if tc.reachable == nil {
				require.Nil(t, resp.Tiers.Primary.Reachable)
			} else {
				require.Equal(t, *tc.reachable, *resp.Tiers.Primary.Reachable)
			}
		})
	}
}

// The lifetimes regression: they were read from the router's own viper, but the
// expiration flags are registered on the cache-server cobra command and never on the
// smartrouter one, so both lookups returned zero on every deployment — reported as a
// confident "0s finalized TTL".
func TestCacheStateLifetimesComeFromTheBackend(t *testing.T) {
	t.Run("a backend that owns its policy reports it", func(t *testing.T) {
		_, resp := getCacheState(t, debugMuxDeps{
			cache: &stubCacheStateBackend{state: performance.DebugCacheState{
				Configured: true, Engine: performance.CacheEngineRESP, Address: "redis:6379",
				WhenUnreachable: performance.CacheWhenUnreachableAttempted,
				Lifetimes: &performance.CacheLifetimes{
					FinalizedSeconds: 3600, NonFinalizedSeconds: 0.5, NodeErrorsSeconds: 60,
				},
			}},
		})

		require.NotNil(t, resp.Tiers.Primary.Lifetimes)
		require.Equal(t, float64(3600), resp.Tiers.Primary.Lifetimes.FinalizedSeconds)
		require.Equal(t, 0.5, resp.Tiers.Primary.Lifetimes.NonFinalizedSeconds)
	})

	// A cache-be tier's TTLs are configured in, and applied by, a different pod.
	// Reporting null says so; reporting 0 asserts a TTL this router cannot know and
	// that no deployment actually uses.
	t.Run("a tier whose TTLs live elsewhere reports null, not zero", func(t *testing.T) {
		rr, resp := getCacheState(t, debugMuxDeps{
			cache: &stubCacheStateBackend{state: grpcTier("cache-be:20100", boolPtr(true))},
		})

		require.Nil(t, resp.Tiers.Primary.Lifetimes)
		require.Contains(t, rr.Body.String(), `"lifetimes":null`)
		require.NotContains(t, rr.Body.String(), `"finalized_seconds":0`,
			"a TTL this router cannot know must not be reported as zero")
	})
}

// The endpoint is documented read-only and must be read-only in the strict sense: on
// the real gRPC client, CacheActive() reaches getClient(), which spawns
// `go reconnectClient()` — a 3s blocking dial retried every 5s. A monitoring scrape
// would then change the state it measures, and a pod that recovered between polls
// would look reachable BECAUSE of the previous poll.
func TestCacheStateDoesNotProbeTheServingPath(t *testing.T) {
	probed := false
	_, resp := getCacheState(t, debugMuxDeps{
		cache: &stubCacheStateBackend{
			state:  grpcTier("cache-be:20100", boolPtr(false)),
			probed: &probed,
		},
	})

	require.False(t, probed,
		"reading cache state must not call the serving-path liveness probe, which dials")
	require.True(t, resp.Tiers.Primary.Configured, "the tier was still reported")
}

// Wire-key contract. Consumers key on these exact strings, and omitempty would erase
// the difference between "the router cannot name this" and "the field does not
// apply" — the distinction this endpoint exists to make.
func TestCacheStateWireContract(t *testing.T) {
	rr, resp := getCacheState(t, debugMuxDeps{
		cache: &stubCacheStateBackend{state: grpcTier("cache-be:20100", boolPtr(true))},
	})

	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	require.Equal(t, 1, resp.SchemaVersion)

	body := rr.Body.String()
	for _, key := range []string{
		`"schema_version"`, `"engine"`, `"tiers"`, `"primary"`, `"secondary"`,
		`"configured"`, `"address"`, `"reachable"`, `"reachable_checked_at"`,
		`"reachable_detail"`, `"when_unreachable"`, `"lifetimes"`,
	} {
		require.Contains(t, body, key, "wire key must be present")
	}

	t.Run("an unnameable address is reported empty, not omitted", func(t *testing.T) {
		rr, _ := getCacheState(t, debugMuxDeps{
			cache: &stubCacheStateBackend{state: performance.DebugCacheState{
				Configured: true, Engine: performance.CacheEngineRESP,
				WhenUnreachable: performance.CacheWhenUnreachableAttempted,
			}},
		})
		require.Contains(t, rr.Body.String(), `"address":""`,
			"a configured tier the router cannot name is a real condition, not an absence")
	})

	t.Run("a snapshot reachability carries its timestamp", func(t *testing.T) {
		checked := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
		_, resp := getCacheState(t, debugMuxDeps{
			cache: &stubCacheStateBackend{state: performance.DebugCacheState{
				Configured: true, Engine: performance.CacheEngineRESP, Address: "redis:6379",
				Reachable: boolPtr(true), CheckedAt: checked, Detail: "no error reported",
				WhenUnreachable: performance.CacheWhenUnreachableAttempted,
			}},
		})
		require.Equal(t, "2026-09-09T12:00:00Z", resp.Tiers.Primary.ReachableCheckedAt,
			"a periodic snapshot must say when it was taken")
		require.Equal(t, "no error reported", resp.Tiers.Primary.ReachableDetail)
	})

	t.Run("a live reading carries no timestamp", func(t *testing.T) {
		_, resp := getCacheState(t, debugMuxDeps{
			cache: &stubCacheStateBackend{state: grpcTier("cache-be:20100", boolPtr(true))},
		})
		require.Empty(t, resp.Tiers.Primary.ReachableCheckedAt)
	})
}

func TestCacheStateRejectsNonGet(t *testing.T) {
	mux := buildDebugMux(debugMuxDeps{})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/debug/cache-state", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// The primary is read from deps.cache — the same field /debug/reset-all flushes.
// Holding a second copy let a fixture wire one and leave the other zero, so the two
// endpoints on one mux could contradict each other about whether a cache exists.
func TestCacheStateAndResetAllSeeTheSameCache(t *testing.T) {
	var offsetNano atomic.Int64
	deps := debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      &stubCacheStateBackend{state: grpcTier("cache-be:20100", boolPtr(true))},
	}

	_, resp := getCacheState(t, deps)
	require.True(t, resp.Tiers.Primary.Configured)

	rr := postResetAllRouter(buildDebugMux(deps))
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), `"cache-be"`,
		"the endpoint that flushes and the endpoint that reports must agree a cache exists")
}
