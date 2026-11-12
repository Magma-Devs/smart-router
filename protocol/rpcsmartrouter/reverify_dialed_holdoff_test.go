package rpcsmartrouter

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// newLimitedTon is a TON node that answers 429 to every path, counting the requests by path.
func newLimitedTon(t *testing.T) (*httptest.Server, func(path string) int) {
	t.Helper()
	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":"rate limited"}`)
	}))
	t.Cleanup(srv.Close)
	return srv, func(path string) int {
		mu.Lock()
		defer mu.Unlock()
		return hits[path]
	}
}

func tontReverifyInputs(t *testing.T, provider *routersession.RPCStaticProviderEndpoint, holdoffs *holdoff.Registry) (*chainReverifyInputs, *admissionRegistry) {
	t.Helper()
	admissions := newAdmissionRegistry()
	return &chainReverifyInputs{
		chainParser:                tontParser(t),
		rpcEndpoint:                &routersession.RPCEndpoint{ChainID: "TONT", ApiInterface: spectypes.APIInterfaceRest},
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{provider},
		recordAdmission:            admissions.record,
		recordReverifiedAdmission:  admissions.recordReverified,
		admissionFor:               admissions.admissionFor,
		rateLimitHoldoff:           holdoffs,
	}, admissions
}

// Since MAG-4214 a re-verify pass verifies the urls the router dials: on a chain with internal
// paths a bare url is one url per path. The hold-off reads and writes the same list, so the
// url a 429 came from, the url a relay hold is on, and the url the pass skips are one url
// (found in review, PR #618). On TONT a bare base url A dials A/v2 and A/v3.
func TestReverifyHoldoff_UsesTheDialedURLs(t *testing.T) {
	t.Run("a 429 from a generated url is held on that url", func(t *testing.T) {
		a := newFakeTon(t, tontRootHash)
		c, _ := newLimitedTon(t)
		provider := tontProvider(commonlib.NodeUrl{Url: a.srv.URL}, commonlib.NodeUrl{Url: c.URL})
		key := holdoff.ProviderKey("TONT", spectypes.APIInterfaceRest, provider.Name)
		holdoffs := holdoff.NewRegistry()
		inputs, admissions := tontReverifyInputs(t, provider, holdoffs)
		active := map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession(provider.Name)}

		applyReverification(t.Context(), inputs, active, reverifyTierStatic, 100)
		require.NotNil(t, admissions.admissionFor(provider).URLRateLimit(2), "the pass met the 429 on dialed index 2 (C/v2)")
		require.True(t, holdoffs.HeldOff(key, c.URL+"/v2"), "the url that answered 429 is held")
		require.False(t, holdoffs.HeldOff(key, a.srv.URL+"/v2"), "A/v2 answered")
	})

	t.Run("a relay hold on a generated url is honoured", func(t *testing.T) {
		a := newFakeTon(t, tontRootHash)
		c, hits := newLimitedTon(t)
		provider := tontProvider(commonlib.NodeUrl{Url: a.srv.URL}, commonlib.NodeUrl{Url: c.URL})
		key := holdoff.ProviderKey("TONT", spectypes.APIInterfaceRest, provider.Name)
		holdoffs := holdoff.NewRegistry()
		holdoffs.RecordRateLimit(key, c.URL+"/v2", 0) // what the relay path records for C/v2
		inputs, _ := tontReverifyInputs(t, provider, holdoffs)
		active := map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession(provider.Name)}

		applyReverification(t.Context(), inputs, active, reverifyTierStatic, 100)
		require.Zero(t, hits("/v2/getMasterchainInfo"), "the held url is not asked")
		require.NotZero(t, a.asked("/v2/getMasterchainInfo"), "the free url is")
	})

	t.Run("a held pinned url is skipped, not a free sibling", func(t *testing.T) {
		a := newFakeTon(t, tontRootHash)
		b, hits := newLimitedTon(t)
		pinned := commonlib.NodeUrl{Url: b.URL + "/v2", InternalPath: "/v2"}
		provider := tontProvider(commonlib.NodeUrl{Url: a.srv.URL}, pinned)
		key := holdoff.ProviderKey("TONT", spectypes.APIInterfaceRest, provider.Name)
		holdoffs := holdoff.NewRegistry()
		holdoffs.RecordRateLimit(key, pinned.Url, 0)
		inputs, _ := tontReverifyInputs(t, provider, holdoffs)
		active := map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession(provider.Name)}

		applyReverification(t.Context(), inputs, active, reverifyTierStatic, 100)
		require.Zero(t, hits("/v2/getMasterchainInfo"), "the held url B/v2 is not asked")
		require.NotZero(t, a.asked("/v2/getMasterchainInfo"), "A/v2 is asked")
	})

	t.Run("the retry wait reads the dialed urls", func(t *testing.T) {
		const base = "https://ton.example"
		provider := tontProvider(commonlib.NodeUrl{Url: base})
		parser := tontParser(t)
		key := holdoff.ProviderKey("TONT", spectypes.APIInterfaceRest, provider.Name)
		require.Equal(t, []holdoffEntry{{provider: key, url: base + "/v2"}, {provider: key, url: base + "/v3"}},
			holdoffEntries(key, provider, parser), "heldOffUntil and the pass read the same entries")
		reg := holdoff.NewRegistry()
		reg.RecordRateLimit(key, base+"/v2", 0)
		require.True(t, heldOffUntil(reg, provider, parser).IsZero(), "/v3 is free, so a retry need not wait")
	})
}
