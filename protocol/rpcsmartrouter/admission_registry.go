package rpcsmartrouter

import (
	"sync"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// admissionRegistry holds the admission last recorded for each provider of one
// chain (MAG-3326). Boot, the failed-provider retry and every epoch re-validation
// record into it, so a refusal never outlives the failure that caused it, and the
// endpoint builder and the subscription tiers read from it.
//
// Guarded because those paths do not all hold the router lock.
type admissionRegistry struct {
	mu         sync.RWMutex
	admissions map[*lavasession.RPCStaticProviderEndpoint]chainlib.ProviderAdmission
	// strikes counts, per provider and node-url position, the epoch passes in a
	// row that refused a url admitted before them.
	strikes map[*lavasession.RPCStaticProviderEndpoint]map[int]int
}

func newAdmissionRegistry() *admissionRegistry {
	return &admissionRegistry{
		admissions: map[*lavasession.RPCStaticProviderEndpoint]chainlib.ProviderAdmission{},
		strikes:    map[*lavasession.RPCStaticProviderEndpoint]map[int]int{},
	}
}

func (r *admissionRegistry) admissionFor(provider *lavasession.RPCStaticProviderEndpoint) chainlib.ProviderAdmission {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.admissions[provider]
}

// record stores a boot or retry pass's admission as it stands, and reports whether
// the admitted set moved.
func (r *admissionRegistry) record(provider *lavasession.RPCStaticProviderEndpoint, admission chainlib.ProviderAdmission) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.strikes, provider)
	return r.store(provider, admission)
}

// recordReverified gives a url the grace a whole provider gets (MAG-2445): one that
// fails for the first time keeps its place until it has failed
// reverifyDemoteThreshold epoch passes running.
func (r *admissionRegistry) recordReverified(provider *lavasession.RPCStaticProviderEndpoint, admission chainlib.ProviderAdmission) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.admissions[provider]
	strikes := r.strikes[provider]
	if strikes == nil {
		strikes = map[int]int{}
		r.strikes[provider] = strikes
	}
	var graced []int
	for _, index := range admission.RefusedURLs() {
		if previous.URLRefused(index) {
			continue
		}
		strikes[index]++
		if strikes[index] < reverifyDemoteThreshold {
			graced = append(graced, index)
		}
	}
	for index := range strikes {
		if !admission.URLRefused(index) {
			delete(strikes, index)
		}
	}
	return r.store(provider, admission.WithoutURLRefusals(graced))
}

// store reports whether the admitted set moved. applyReverification rebuilds a
// provider's session on it: an active session is refreshed from its OWN endpoints,
// so recording a new admission is not by itself enough to get a recovered service
// back into the pairing. A first record that refuses anything has moved too — a
// session built before it holds every url.
func (r *admissionRegistry) store(provider *lavasession.RPCStaticProviderEndpoint, admission chainlib.ProviderAdmission) bool {
	previous, seen := r.admissions[provider]
	// Assigned unconditionally, including an empty admission: that is how a
	// service that has recovered gets un-refused.
	r.admissions[provider] = admission
	if !seen {
		return admission.Any()
	}
	return !previous.Equal(admission)
}
