package rpcsmartrouter

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// Admission verifies every configured url over its own connection, so each verdict
// lands on the url that earned it, whatever order the urls are configured in.

func TestValidateProviderCollections_DropsTheDeadUrlAndKeepsTheProvider(t *testing.T) {
	for _, deadFirst := range []bool{true, false} {
		up := newFakeUpstreams(t)
		dead := commonlib.NodeUrl{Url: up.httpURL("/a/" + upDead)}
		good := commonlib.NodeUrl{Url: up.httpURL("/a/" + upArchive)}
		urls, deadAt, goodAt := []commonlib.NodeUrl{good, dead}, 1, 0
		if deadFirst {
			urls, deadAt, goodAt = []commonlib.NodeUrl{dead, good}, 0, 1
		}
		admission, err := validateProviderCollections(t.Context(), staticProvider("ETH1", urls...), liveParser(t, "ETH1", true), 20*time.Second)
		require.NoError(t, err, "the good url still serves the root path")
		require.True(t, admission.URLRefused(deadAt))
		require.False(t, admission.URLRefused(goodAt))
		require.Contains(t, up.methods("http", "/a/"+upDead), "eth_chainId", "the dead url was asked, not skipped")
		require.Contains(t, up.methods("http", "/a/"+upArchive), "eth_chainId")
	}
}

// Urls on different connections are asked side by side: one that hangs spends its
// own deadline, and the one beside it answers in its own time, in either order.
func TestValidateProviderCollections_ASlowUrlStarvesNoOther(t *testing.T) {
	for _, slowFirst := range []bool{true, false} {
		up := newFakeUpstreams(t)
		slow := commonlib.NodeUrl{Url: up.httpURL("/b/" + upHang)}
		good := commonlib.NodeUrl{Url: up.httpURL("/b/" + upArchive)}
		urls, slowAt := []commonlib.NodeUrl{good, slow}, 1
		if slowFirst {
			urls, slowAt = []commonlib.NodeUrl{slow, good}, 0
		}
		start := time.Now()
		admission, err := validateProviderCollections(t.Context(), staticProvider("ETH1", urls...), liveParser(t, "ETH1", true), 3*time.Second)
		require.NoError(t, err, "the good url answered within the deadline")
		require.True(t, admission.URLRefused(slowAt))
		require.False(t, admission.URLRefused(1-slowAt))
		require.Less(t, time.Since(start), 8*time.Second)
	}
}

// A socket that accepts TCP and never upgrades costs the socket, not the provider.
func TestValidateProviderCollections_StalledSocketCostsOnlyTheSocket(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: stalledSocket(t)}, commonlib.NodeUrl{Url: up.httpURL("/c/" + upArchive)})
	admission, err := validateProviderCollections(t.Context(), provider, liveParser(t, "ETH1", true), 3*time.Second)
	require.NoError(t, err)
	require.True(t, admission.URLRefused(0))
	require.False(t, admission.URLRefused(1))
}

// A socket that cannot be dialed costs the socket: unary traffic never used it.
func TestValidateProviderCollections_DeadSocketCostsOnlyTheSocket(t *testing.T) {
	up := newFakeUpstreams(t)
	gone := httptest.NewServer(nil)
	deadSocket := commonlib.NodeUrl{Url: "ws" + strings.TrimPrefix(gone.URL, "http") + "/ws"}
	gone.Close()

	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/d/" + upArchive)}, deadSocket)
	admission, err := validateProviderCollections(t.Context(), provider, liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	require.True(t, admission.URLRefused(1))

	admissionFor := func(*lavasession.RPCStaticProviderEndpoint) chainlib.ProviderAdmission { return admission }
	require.Empty(t, collectWSEndpoints([]*lavasession.RPCStaticProviderEndpoint{provider}, admissionFor, ""),
		"a refused socket gets no subscription")
	require.Len(t, collectWSEndpoints([]*lavasession.RPCStaticProviderEndpoint{provider}, nil, ""), 1,
		"configured-ness is not admission")
	require.Equal(t, []commonlib.NodeUrl{provider.NodeUrls[0]}, admittedNodeUrls(provider, admission))
}

// A socket's own checks run over the socket; with websocket verification on, one that
// answers only subscriptions is refused alone.
func TestValidateProviderCollections_SocketIsCheckedOverTheSocket(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/e/" + upArchive)}, commonlib.NodeUrl{Url: up.wsURL("/e/" + upSubOnly)})

	admission, err := validateProviderCollections(t.Context(), provider, liveParser(t, "ETH1", false), 20*time.Second)
	require.NoError(t, err)
	require.True(t, admission.URLRefused(1))
	require.Contains(t, up.methods("ws", "/e/"+upSubOnly), "eth_chainId")
	require.NotContains(t, up.methods("http", "/e/"+upArchive), "eth_subscribe")

	admission, err = validateProviderCollections(t.Context(), provider, liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	require.False(t, admission.Any(), "--skip-websocket-verification only dials the socket")
}

// Checks reach the url they belong to: a separately hosted archive url is asked for
// the base collection it also serves, and fails its own archive check.
func TestValidateProviderCollections_ArchiveOnItsOwnHost(t *testing.T) {
	up := newFakeUpstreams(t)
	base := commonlib.NodeUrl{Url: up.httpURL("/f/base/" + upArchive)}
	archive := commonlib.NodeUrl{Url: up.httpURL("/f/arch/" + upPruned), Addons: []string{"archive"}}
	admission, err := validateProviderCollections(t.Context(), staticProvider("ETH1", base, archive), liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	require.False(t, admission.URLRefused(1), "the pruned node serves the base collection")
	services, keep := admission.AdmittedServices(archive)
	require.True(t, keep)
	require.Empty(t, services, "archive is refused on the node that failed it")
	require.Contains(t, up.methods("http", "/f/arch/"+upPruned), "eth_chainId", "the archive url answered for the base collection")
	require.Contains(t, up.methods("http", "/f/base/"+upArchive), "eth_chainId", "the base url answered for itself")
}

// A rate limit refuses nothing: the url is kept, and the admission says it was throttled.
func TestValidateProviderCollections_ARateLimitRefusesNothing(t *testing.T) {
	up := newFakeUpstreams(t)
	admission, err := validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/g/" + upArchive)},
		commonlib.NodeUrl{Url: up.httpURL("/g/" + upThrottled)},
	), liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	require.False(t, admission.URLRefused(1))
	require.True(t, isRateLimitFailure(admission.Throttled()))
}

// Every url of the only path dead: the provider is excluded, with a cause re-verification
// can classify — a rate limit only when that is all there was.
func TestValidateProviderCollections_NoUrlLeft(t *testing.T) {
	up := newFakeUpstreams(t)
	parser := liveParser(t, "ETH1", true)

	_, err := validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/h/" + upDead)},
	), parser, 20*time.Second)
	require.ErrorContains(t, err, "no node url left serving the root path")
	require.False(t, isRateLimitFailure(err))

	_, err = validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/i/" + upThrottled)},
	), parser, 20*time.Second)
	require.True(t, isRateLimitFailure(err), "a provider that was only throttled reads as inconclusive: %v", err)

	_, err = validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/j/" + upThrottled)},
		commonlib.NodeUrl{Url: up.httpURL("/j/" + upDead)},
	), parser, 20*time.Second)
	require.True(t, isRateLimitFailure(err), "no url on the path verified and one was only throttled: inconclusive, %v", err)
}

// The router refuses these shapes before dialing anything.
func TestValidateProviderCollections_ShapeRefusals(t *testing.T) {
	up := newFakeUpstreams(t)
	_, err := validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.wsURL("/k/" + upArchive)},
	), liveParser(t, "ETH1", true), 20*time.Second)
	require.ErrorContains(t, err, "HTTP/HTTPS is mandatory")

	_, err = validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/l/" + upArchive)},
	), liveParser(t, "ETH1", false), 20*time.Second)
	require.ErrorContains(t, err, "websocket is not provided")
	require.Empty(t, up.methods("http", "/l/"+upArchive), "a refused shape dials nothing")
}

// An addon and an extension on one url verify together, each check charged to its own.
func TestValidateProviderCollections_AddonAndExtensionOnOneUrl(t *testing.T) {
	up := newFakeUpstreams(t)
	url := commonlib.NodeUrl{Url: up.httpURL("/m/" + upArchive), Addons: []string{"archive", "debug"}}
	admission, err := validateProviderCollections(t.Context(), staticProvider("ETH1", url), liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	services, keep := admission.AdmittedServices(url)
	require.True(t, keep)
	require.Equal(t, []string{"archive"}, services, "debug_getRawHeader is not served here, so debug is refused on its own")
	require.Contains(t, up.methods("http", "/m/"+upArchive), "debug_getRawHeader")
}

// Two entries of one url are judged apart when they differ in more than their
// services: the keyless copy's refusal does not reach the keyed one.
func TestValidateProviderCollections_EntriesAreJudgedApart(t *testing.T) {
	up := newFakeUpstreams(t)
	keyed := commonlib.NodeUrl{Url: up.httpURL("/n/" + upAuthed), AuthConfig: commonlib.AuthConfig{AuthHeaders: map[string]string{upAuthHeader: upAuthKey}}}
	keyless := commonlib.NodeUrl{Url: keyed.Url, Addons: []string{"debug"}}
	admission, err := validateProviderCollections(t.Context(), staticProvider("ETH1", keyed, keyless), liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	require.False(t, admission.URLRefused(0))
	require.True(t, admission.URLRefused(1))
}

// Entries that share a connection are dialed once and asked each check once.
func TestValidateProviderCollections_ASharedConnectionIsAskedOnce(t *testing.T) {
	up := newFakeUpstreams(t)
	url := up.httpURL("/o/" + upArchive)
	_, err := validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: url},
		commonlib.NodeUrl{Url: url, Addons: []string{"archive"}},
		commonlib.NodeUrl{Url: url, Addons: []string{"debug"}},
	), liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	chainIDs := 0
	for _, method := range up.methods("http", "/o/"+upArchive) {
		if method == "eth_chainId" {
			chainIDs++
		}
	}
	require.Equal(t, 1, chainIDs, "three entries of one url share one chain-id check")
}

// standalone-addons names add-on collections: a url declaring only extensions serves
// the base collection, as its relay endpoint does, so it is checked for it.
func TestValidateProviderCollections_StandaloneExtensionOnly(t *testing.T) {
	up := newFakeUpstreams(t)
	_, err := validateProviderCollections(t.Context(), staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/s/" + upArchive), Addons: []string{"archive"}, StandaloneAddons: true},
	), liveParser(t, "ETH1", true), 20*time.Second)
	require.NoError(t, err)
	require.Contains(t, up.methods("http", "/s/"+upArchive), "eth_chainId")
}

func TestAdmittedNodeUrls_KeepsOrderAndIdentityWhenNothingIsRefused(t *testing.T) {
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: "https://a"}, commonlib.NodeUrl{Url: "https://b"})
	kept := admittedNodeUrls(provider, chainlib.ProviderAdmission{})
	require.True(t, &kept[0] == &provider.NodeUrls[0], "no allocation when nothing was refused")
}

// An epoch pass that met a rate limit on one url keeps the provider paired as it was,
// leaves the url's standing alone, and holds the provider off.
func TestApplyReverification_ARateLimitedUrlBacksOffAndChangesNothing(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/p/" + upArchive)},
		commonlib.NodeUrl{Url: up.httpURL("/p/" + upThrottled)},
	)
	admissions := newAdmissionRegistry()
	holdoffs := holdoff.NewRegistry()
	inputs := &chainReverifyInputs{
		chainParser:                liveParser(t, "ETH1", true),
		rpcEndpoint:                &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{provider},
		recordAdmission:            admissions.record,
		recordReverifiedAdmission:  admissions.recordReverified,
		admissionFor:               admissions.admissionFor,
		rateLimitHoldoff:           holdoffs,
	}
	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession(provider.Name)}

	next, demoted, promoted := applyReverification(t.Context(), inputs, active, reverifyTierStatic, 100)
	require.Contains(t, collectNames(next), provider.Name)
	require.Empty(t, demoted)
	require.Empty(t, promoted, "nothing moved, so nothing is rebuilt")
	require.False(t, admissions.admissionFor(provider).Any())
	require.True(t, holdoffs.HeldOff(provider.Name, holdoffURLKey(provider)), "the vendor's limit is honoured on the next pass")
}

// The retry loop records what it admitted, so the provider comes back without the
// url it found dead.
func TestRevalidateTier_RecordsTheAdmission(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/q/" + upArchive)},
		commonlib.NodeUrl{Url: up.httpURL("/q/" + upDead)},
	)
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}
	admissions := newAdmissionRegistry()
	rpsr := &RPCSmartRouter{reverifyInputs: map[string]*chainReverifyInputs{
		rpcEndpoint.Key(): {recordAdmission: admissions.record},
	}}

	recovered, stillFailed := rpsr.revalidateTier(t.Context(), []*lavasession.RPCStaticProviderEndpoint{provider},
		liveParser(t, "ETH1", true), rpcEndpoint, reverifyTierStatic)
	require.Len(t, recovered, 1)
	require.Empty(t, stillFailed)
	require.True(t, admissions.admissionFor(provider).URLRefused(1))
}
