package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// One pass where url A answers and B and C answer 429. B and C held at once escalate the entry,
// and A's answer drops that escalation only when it is recorded after them. Recording every
// rate limit first makes the result the same in every config order: A free, B and C held.
func TestRecordPassHoldoffs_AnAnsweringURLIsFreeInEveryConfigOrder(t *testing.T) {
	const key = "ETH1|jsonrpc|p"
	answered := urlPassResult{answered: true}
	limited := urlPassResult{throttle: common.StatusCodeError429}
	for name, order := range map[string][]string{
		"answering url first": {"https://a", "https://b", "https://c"},
		"answering url last":  {"https://b", "https://c", "https://a"},
	} {
		t.Run(name, func(t *testing.T) {
			reg := holdoff.NewRegistry()
			entries := make([]holdoffEntry, len(order))
			results := make([]urlPassResult, len(order))
			for index, url := range order {
				entries[index] = holdoffEntry{provider: key, url: url}
				results[index] = limited
				if url == "https://a" {
					results[index] = answered
				}
			}
			recordPassHoldoffs(reg, "ETH1", "p", entries, results)
			require.False(t, reg.HeldOff(key, "https://a"), "A answered in this pass")
			require.True(t, reg.HeldOff(key, "https://b"))
			require.True(t, reg.HeldOff(key, "https://c"))
		})
	}
}

// The same url configured twice — base, and a standalone debug add-on — is one url to the
// hold-off. A 429 on its base checks is not cleared by the add-on entry counting as answered.
func TestRecordPassHoldoffs_TheSameURLTwiceKeepsItsRateLimit(t *testing.T) {
	const key = "ETH1|jsonrpc|p"
	reg := holdoff.NewRegistry()
	entries := []holdoffEntry{{provider: key, url: "https://a"}, {provider: key, url: "https://a"}}
	results := []urlPassResult{{throttle: common.StatusCodeError429}, {answered: true}}
	recordPassHoldoffs(reg, "ETH1", "p", entries, results)
	require.True(t, reg.HeldOff(key, "https://a"))
}

// A websocket url's 429 is held under the url alone, the key the WS subscription path uses.
// It does not count toward the entry's escalation, so with one https url limited and the
// socket limited, the entry's other https url stays free for relays.
func TestRecordPassHoldoffs_AWebsocketRateLimitDoesNotHoldTheEntry(t *testing.T) {
	provider := staticProvider("ETH1",
		common.NodeUrl{Url: "https://a"},
		common.NodeUrl{Url: "wss://a"},
		common.NodeUrl{Url: "https://b"},
	)
	key := holdoff.ProviderKey("ETH1", "jsonrpc", provider.Name)
	entries := holdoffEntries(key, provider)
	require.Equal(t, holdoffEntry{provider: "wss://a", url: "wss://a"}, entries[1])

	reg := holdoff.NewRegistry()
	results := []urlPassResult{
		{throttle: common.StatusCodeError429},
		{throttle: common.StatusCodeError429},
		{}, // no answer (a timeout): changes nothing
	}
	recordPassHoldoffs(reg, "ETH1", provider.Name, entries, results)
	require.True(t, reg.HeldOff(key, "https://a"))
	require.True(t, reg.HeldOff("wss://a", "wss://a"), "the socket is held where the WS path looks")
	require.False(t, reg.HeldOff(key, "https://b"), "one held relay url does not escalate the entry")
	_, held := reg.ProviderReadyAt(key, []string{"https://a", "https://b"})
	require.False(t, held)
}

// A url the pass did not ask keeps its hold-off and its strikes as they were.
func TestRecordPassHoldoffs_AURLNotAskedChangesNothing(t *testing.T) {
	const key = "ETH1|jsonrpc|p"
	reg := holdoff.NewRegistry()
	reg.RecordRateLimit(key, "https://a", 0)
	before := reg.ReadyAt(key, "https://a")
	recordPassHoldoffs(reg, "ETH1", "p", []holdoffEntry{{provider: key, url: "https://a"}}, []urlPassResult{{notAsked: true}})
	require.Equal(t, before, reg.ReadyAt(key, "https://a"))
}

// A url held off after a rate limit is not asked, and the provider's other urls still are: the
// pass is not skipped because one url is busy (MAG-4733).
func TestApplyReverification_AHeldURLIsNotAskedAndItsSiblingIs(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1",
		common.NodeUrl{Url: up.httpURL("/a/" + upArchive)},
		common.NodeUrl{Url: up.httpURL("/b/" + upArchive)},
	)
	key := holdoff.ProviderKey("ETH1", "jsonrpc", provider.Name)
	holdoffs := holdoff.NewRegistry()
	holdoffs.RecordRateLimit(key, provider.NodeUrls[1].Url, 0)
	heldUntil := holdoffs.ReadyAt(key, provider.NodeUrls[1].Url)
	admissions := newAdmissionRegistry()
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

	next, demoted, _ := applyReverification(t.Context(), inputs, active, reverifyTierStatic, 100)
	require.Contains(t, collectNames(next), provider.Name)
	require.Empty(t, demoted)
	require.NotEmpty(t, up.methods("http", "/a/"+upArchive), "the free url is checked")
	require.Empty(t, up.methods("http", "/b/"+upArchive), "the held url is not sent a request")
	require.Equal(t, heldUntil, holdoffs.ReadyAt(key, provider.NodeUrls[1].Url), "its hold-off is left as it was")
	require.False(t, admissions.admissionFor(provider).Any())
}

// Every url held: the pass is skipped, and nothing is asked.
func TestApplyReverification_EveryURLHeldSkipsThePass(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1",
		common.NodeUrl{Url: up.httpURL("/a/" + upArchive)},
		common.NodeUrl{Url: up.httpURL("/b/" + upArchive)},
	)
	key := holdoff.ProviderKey("ETH1", "jsonrpc", provider.Name)
	holdoffs := holdoff.NewRegistry()
	holdoffs.RecordRateLimit(key, provider.NodeUrls[0].Url, 0)
	holdoffs.RecordRateLimit(key, provider.NodeUrls[1].Url, 0)
	admissions := newAdmissionRegistry()
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

	next, demoted, _ := applyReverification(t.Context(), inputs, active, reverifyTierStatic, 100)
	require.Contains(t, collectNames(next), provider.Name, "a skipped pass is inconclusive")
	require.Empty(t, demoted)
	require.Empty(t, up.methods("http", "/a/"+upArchive))
	require.Empty(t, up.methods("http", "/b/"+upArchive))
}

// The case MAG-4733 is about: url B was refused, its node has recovered, and url A is busy
// answering relays with 429s. B comes back at the next pass instead of waiting for A.
func TestApplyReverification_ARecoveredURLComesBackWhileItsSiblingIsHeld(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1",
		common.NodeUrl{Url: up.httpURL("/a/" + upArchive)},
		common.NodeUrl{Url: up.httpURL("/b/" + upDead)},
	)
	parser := liveParser(t, "ETH1", true)
	admissions := newAdmissionRegistry()
	boot, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
	require.NoError(t, err)
	require.True(t, boot.URLRefused(1), "B is refused at boot")
	admissions.record(provider, boot)

	provider.NodeUrls[1].Url = up.httpURL("/b/" + upArchive) // B's node recovers
	key := holdoff.ProviderKey("ETH1", "jsonrpc", provider.Name)
	holdoffs := holdoff.NewRegistry()
	holdoffs.RecordRateLimit(key, provider.NodeUrls[0].Url, 0) // A is busy
	inputs := &chainReverifyInputs{
		chainParser:                parser,
		rpcEndpoint:                &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{provider},
		recordAdmission:            admissions.record,
		recordReverifiedAdmission:  admissions.recordReverified,
		admissionFor:               admissions.admissionFor,
		rateLimitHoldoff:           holdoffs,
	}
	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession(provider.Name)}

	askedAtBoot := len(up.methods("http", "/a/"+upArchive))
	_, _, promoted := applyReverification(t.Context(), inputs, active, reverifyTierStatic, 100)
	require.Contains(t, promoted, provider.Name, "the session is rebuilt with B in it")
	require.False(t, admissions.admissionFor(provider).URLRefused(1), "B is admitted again")
	require.False(t, admissions.admissionFor(provider).URLRefused(0), "A keeps its last verdict")
	require.Len(t, up.methods("http", "/a/"+upArchive), askedAtBoot, "A is not sent a request by the pass")
	require.True(t, holdoffs.HeldOff(key, provider.NodeUrls[0].Url))
}
