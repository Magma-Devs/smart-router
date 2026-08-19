package rpcsmartrouter

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

// SpecReVerifyInterval is how often the background pass re-probes configured providers.
//
// Re-verification used to ride the epoch tick, so it ran every epoch — 15 minutes. That
// cadence was inherited from the hook, not chosen: a verification answers "does this
// upstream still serve what it declares", and that changes when a vendor changes their
// config, not every quarter hour. An hour keeps the answer fresh at a fraction of the
// request volume.
//
// Note this stretches demote latency. reverifyDemoteThreshold consecutive failures now
// span 2 × this interval rather than 2 epochs. Demotion was always the coarse cleanup —
// endpoint health disables a dead endpoint within seconds and QoS stops routing to it —
// so trading demote latency for a fleet that stops rate-limiting itself is the right way
// round. Exported for tests; not flag-bound.
var SpecReVerifyInterval = time.Hour

// SpecReVerifyJitter is the window each refresh is spread over. Every instance picks a
// pseudo-random point inside it, so the fleet's probes land scattered rather than together.
//
// It is deliberately separate from, and much smaller than, the interval. The interval says
// how fresh the answer has to be; the jitter says how tightly the fleet is allowed to bunch
// up. A couple of minutes is already ample: spreading ~80 instances over 2 minutes puts the
// fleet in the single-digit rps range against a cap of hundreds, while keeping "verification
// happens shortly after the interval elapses" true enough to reason about in logs.
//
// Bound to --verifications-jitter. Zero disables jitter entirely (every instance probes as
// soon as its interval elapses), which is almost never what you want with more than one
// instance sharing an upstream account.
var SpecReVerifyJitter = 2 * time.Minute

// errNoReVerifyResultYet marks a provider the background pass has not probed yet. It is
// deliberately distinct from both nil (healthy) and a validation failure: at the first
// epoch boundary after start-up there is no evidence either way, and the reconciliation
// must not read absence as either health or failure.
var errNoReVerifyResultYet = errors.New("re-verify: no completed result yet")

// reverifyResults is the hand-off between the background prober and the epoch boundary.
//
// The boundary reads the last completed set and returns immediately; the prober refreshes
// it on its own jittered schedule. This is what decouples verification load from the epoch
// tick — see reverifyStartOffset for why that matters.
type reverifyResults struct {
	mu      sync.RWMutex
	results map[string]error // provider name -> last validation outcome (nil = healthy)
}

func newReverifyResults() *reverifyResults {
	return &reverifyResults{results: map[string]error{}}
}

// get returns the last completed outcome for a provider, or errNoReVerifyResultYet.
func (r *reverifyResults) get(name string) error {
	if r == nil {
		return errNoReVerifyResultYet
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	err, ok := r.results[name]
	if !ok {
		return errNoReVerifyResultYet
	}
	return err
}

func (r *reverifyResults) set(name string, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results[name] = err
}

// reverifyJitter returns this instance's offset into the jitter window for a given tick.
//
// Deterministic in (seed, tick) rather than drawn from rand: the sequence is reproducible,
// so an operator reading logs can work out when an instance probed and when it will next.
// Re-derived per tick rather than fixed once, so two instances whose seeds happen to land
// close together drift apart on the following cycle instead of colliding forever.
func reverifyJitter(window time.Duration, seed string, tick uint64) time.Duration {
	if window <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], tick)
	_, _ = h.Write(buf[:])
	return time.Duration(h.Sum64() % uint64(window))
}

// reverifySeed identifies this router instance for jitter purposes. It is the hostname
// combined with a checksum over the routing configuration, and it needs both halves.
//
// A config checksum alone does not work. Replicas of one chain run byte-identical config --
// same chains, same providers, same listen port inside each container -- so every replica
// would hash to the same offset and probe together. On a fleet of N chains x 2 replicas that
// throws away half the spread.
//
// A hostname alone does not work either. It is unique per pod under Kubernetes and nowhere
// else: run several routers on one host -- compose, bare metal, one process per chain on a VM
// -- and every one of them hashes to the same offset, exactly as synchronized as having no
// jitter at all. That was the bug in the first revision of this file.
//
// Together they cover both: the hostname separates replicas of one config, the checksum
// separates co-located processes running different configs. Both halves are config- or
// environment-derived rather than random, so the seed survives a restart and a crashlooping
// instance keeps its slot instead of re-rolling.
//
// Falls back to hostname + PID when no endpoint is known yet -- unique, but it re-rolls on
// restart, which is why it is only the fallback.
func (rpsr *RPCSmartRouter) reverifySeed() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "smart-router"
	}

	rpsr.mu.Lock()
	parts := make([]string, 0, len(rpsr.reverifyInputs))
	for key, in := range rpsr.reverifyInputs {
		if in == nil {
			continue
		}
		entry := key
		if in.rpcEndpoint != nil {
			entry += "@" + in.rpcEndpoint.NetworkAddress
		}
		for _, tier := range [][]*lavasession.RPCStaticProviderEndpoint{in.configuredStatic, in.configuredBackup} {
			for _, prov := range tier {
				if prov == nil {
					continue
				}
				entry += "|" + prov.Name
				for _, u := range prov.NodeUrls {
					entry += ">" + u.UrlStr() + "(" + strings.Join(u.Addons, "+") + ")"
				}
			}
		}
		parts = append(parts, entry)
	}
	rpsr.mu.Unlock()

	if len(parts) == 0 {
		return host + "/pid:" + strconv.Itoa(os.Getpid())
	}
	sort.Strings(parts) // map iteration order must not change the seed
	sum := sha256.Sum256([]byte(strings.Join(parts, ";")))
	return host + "/cfg:" + hex.EncodeToString(sum[:8])
}

// startReVerifyRefresher runs the background probe loop: wait out this pod's offset, then
// refresh every SpecReVerifyInterval. Results land in each chain's reverifyResults, which
// the epoch boundary consumes without doing any network work of its own.
func (rpsr *RPCSmartRouter) startReVerifyRefresher(ctx context.Context) {
	seed := rpsr.reverifySeed()
	first := reverifyJitter(SpecReVerifyJitter, seed, 0)
	utils.LavaFormatInfo("re-verify: background refresher scheduled",
		utils.LogAttr("interval", SpecReVerifyInterval.String()),
		utils.LogAttr("jitterWindow", SpecReVerifyJitter.String()),
		utils.LogAttr("firstRunIn", first.String()),
		utils.LogAttr("seed", seed),
	)

	go func() {
		timer := time.NewTimer(first)
		defer timer.Stop()
		for tick := uint64(1); ; tick++ {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			rpsr.refreshReVerifyResults(ctx)
			// Each cycle lands somewhere in [interval, interval+jitter).
			timer.Reset(SpecReVerifyInterval + reverifyJitter(SpecReVerifyJitter, seed, tick))
		}
	}()
}

// refreshReVerifyResults probes every configured provider on every chain once and stores
// the outcome. It performs no pairing mutation — the epoch boundary owns that.
func (rpsr *RPCSmartRouter) refreshReVerifyResults(ctx context.Context) {
	rpsr.mu.Lock()
	type job struct {
		chainKey string
		inputs   *chainReverifyInputs
	}
	jobs := make([]job, 0, len(rpsr.reverifyInputs))
	for k, in := range rpsr.reverifyInputs {
		if in != nil {
			jobs = append(jobs, job{chainKey: k, inputs: in})
		}
	}
	rpsr.mu.Unlock()

	started := time.Now()
	probed := 0
	for _, j := range jobs {
		validate := j.inputs.validateFn
		if validate == nil {
			validate = func(c context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
				return validateProvider(c, p, j.inputs.chainParser, SpecReVerifyAttemptTimeout)
			}
		}
		configured := append(append([]*lavasession.RPCStaticProviderEndpoint{},
			j.inputs.configuredStatic...), j.inputs.configuredBackup...)

		var wg sync.WaitGroup
		sem := make(chan struct{}, SpecReVerifyConcurrency)
		for _, p := range configured {
			p := p
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			wg.Add(1)
			probed++
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				j.inputs.results.set(p.Name, validate(ctx, p))
			}()
		}
		wg.Wait()
	}

	utils.LavaFormatDebug("re-verify: background refresh complete",
		utils.LogAttr("chains", len(jobs)),
		utils.LogAttr("providersProbed", probed),
		utils.LogAttr("elapsed", time.Since(started).String()),
	)
}
