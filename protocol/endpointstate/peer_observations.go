package endpointstate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The fleet tracker gate (MAG-2981).
//
// Every router pod runs its own per-endpoint ChainTracker, so the tracker's upstream request
// volume scales with the replica count: N pods × M endpoints × (divisor / blockTime). The relay
// traffic gate (MAG-2159) already skips a poll cycle when served traffic keeps the tip fresh;
// this file lets a FRESH OBSERVATION FROM A PEER POD do the same. Each pod publishes every
// successful poll to the cache backend, and before polling it asks the backend whether a peer
// polled the same endpoint recently. If one did, the cycle is skipped and the peer's block is
// adopted into this pod's tip store under SourcePeer, which keeps the ALIVE / keeping-up
// verdicts and the QoS sync reference fed — but only for an endpoint this pod is itself still
// reaching (invariant 2). Latency is never shared: a peer's round-trip says nothing about this
// pod's path to the endpoint.
//
// Four invariants keep it safe:
//   - A pod never borrows its OWN observation (PodId check) — otherwise one poll would throttle
//     every subsequent poll, the self-throttle the relay gate's Source==Relay rule forbids.
//   - A pod only borrows while its OWN last poll SUCCEEDED (localPollHealthy) — an adopted block
//     refreshes ObservedAt, which probing.Verdict reads as `alive`, so a pod that lost its path
//     to an endpoint would otherwise read it ALIVE forever on borrowed evidence.
//   - The tracker's skip budget (chaintracker.maxRelaySkipsBeforePoll) counts peer skips too,
//     so every pod still polls every endpoint locally on a bounded cadence — a pod-local
//     connectivity fault or a lying upstream stays detectable.
//   - Re-enabling a disabled endpoint still requires a LOCAL successful poll
//     (probing.Verdict reads LastSuccessfulPoll, which a peer observation never touches).
//
// With self-exclusion, N pods on one endpoint converge to one real poll per interval plus each
// pod's forced poll every (skip budget + 1) cycles.

// PeerObservationStore is the fleet-side half of the gate. Production wires the cache backend
// (NewCachePeerObservations); tests supply an in-memory fake.
type PeerObservationStore interface {
	// Publish records this pod's successful poll of an endpoint. ttl bounds how long the
	// store keeps it. Errors are advisory — a lost publish costs a peer one real poll.
	Publish(ctx context.Context, chainID, apiInterface, endpointID, podID string, block int64, ttl time.Duration) error
	// Fetch returns the freshest live observation of an endpoint: the block, who published
	// it, and its age on the STORE's clock. found=false is a normal miss.
	Fetch(ctx context.Context, chainID, apiInterface, endpointID string) (block int64, podID string, age time.Duration, found bool, err error)
}

// noteGateSkip forwards a suppressed poll cycle to the OnGateSkip consumer, if any.
func (m *EndpointMonitor) noteGateSkip(endpointURL, source string) {
	if m.onGateSkip != nil {
		m.onGateSkip(endpointURL, source)
	}
}

// noteGateError forwards a failed fleet-store call to the OnGateError consumer, if any.
func (m *EndpointMonitor) noteGateError(endpointURL, op string) {
	if m.onGateError != nil {
		m.onGateError(endpointURL, op)
	}
}

// peerFetchBudget bounds the gate's cache read against THIS monitor's cadence: never more than a
// quarter of the poll interval. The read sits on the poll goroutine, and the next tick is scheduled
// only after the cycle returns, so its latency is ADDED to the interval rather than overlapped with
// it. At the flat 200ms ceiling alone, a slow-but-reachable cache backend would double the cycle
// time of a 200ms-interval chain — the gate would then cost more than the upstream request it
// exists to avoid. A cache that cannot answer within a quarter of the cycle is one to poll past.
func (m *EndpointMonitor) peerFetchBudget() time.Duration {
	if m.flatPollInterval <= 0 {
		return peerFetchTimeout
	}
	return min(peerFetchTimeout, m.flatPollInterval/4)
}

const (
	// peerFetchTimeout bounds the gate's synchronous read on the tracker's poll goroutine. The
	// cache backend is an in-cluster hop; a read that takes longer than this is treated as a
	// miss (poll locally) rather than delaying the cadence further.
	peerFetchTimeout = 200 * time.Millisecond
	// peerPublishTimeout bounds the fire-and-forget publish after a successful poll.
	peerPublishTimeout = time.Second
	// peerObservationTTLMultiplier sizes the published TTL relative to the gate's freshness
	// window: an entry must outlive the window it is judged against, with margin for the
	// reader's own clock-free age check, but not linger for minutes after the pod dies.
	peerObservationTTLMultiplier = 2
)

// EndpointID is the opaque digest a URL is published under. The raw URL carries the provider
// API key, so it never crosses the cache wire or lands in the cache server's logs; every pod
// with the same URL string derives the same digest, which is all the fleet needs to agree on.
func EndpointID(endpointURL string) string {
	sum := sha256.Sum256([]byte(endpointURL))
	return hex.EncodeToString(sum[:16])
}

var (
	localPodIDOnce sync.Once
	localPodID     string
)

// LocalPodID identifies this router process to its peers. Hostname (the pod name under
// Kubernetes) plus a random suffix, so two processes on one host — or a restarted pod reusing
// a name — never mistake each other's observations for their own.
func LocalPodID() string {
	localPodIDOnce.Do(func() {
		host, _ := os.Hostname()
		var b [4]byte
		_, _ = rand.Read(b[:])
		localPodID = host + "/" + hex.EncodeToString(b[:])
	})
	return localPodID
}

// cachePeerObservations adapts the cache backend client to PeerObservationStore.
type cachePeerObservations struct {
	cache *performance.Cache
	// unimplementedOnce rate-limits the warning for a cache backend that predates the
	// observation RPCs (a rolling upgrade window): every call would otherwise log, on every
	// poll tick, for every endpoint. One adapter is built per chain+interface, so the warning
	// is once per listen endpoint, not once per process. The gate degrades to "poll locally"
	// either way; only the LOG is rate-limited, never the error (see warnIfUnimplemented).
	unimplementedOnce sync.Once
}

// warnIfUnimplemented logs once per chain+interface when the cache backend refuses an RPC it does
// not know — the rolling-upgrade window.
//
// It does NOT swallow the error. Every error already degrades to a local poll, so returning it
// changes no behaviour; what it changes is visibility. Swallowed, an out-of-date backend produced
// zero gate errors AND zero peer skips, which reads exactly like a healthy fleet with nothing to
// share — the one state rpc_endpoint_tracker_gate_errors_total was added to expose. Surfaced, it is
// a steady climb at full tick rate, which nothing else looks like.
func (c *cachePeerObservations) warnIfUnimplemented(err error) {
	if status.Code(err) != codes.Unimplemented {
		return
	}
	c.unimplementedOnce.Do(func() {
		utils.LavaFormatWarning("fleet tracker gate: the cache backend does not implement endpoint observations; polling locally until it is upgraded", err)
	})
}

// NewCachePeerObservations wraps the cache backend client as the fleet observation store.
// Returns nil when no cache is configured, which disables the peer gate.
func NewCachePeerObservations(cache *performance.Cache) PeerObservationStore {
	if cache == nil {
		return nil
	}
	return &cachePeerObservations{cache: cache}
}

func (c *cachePeerObservations) Publish(ctx context.Context, chainID, apiInterface, endpointID, podID string, block int64, ttl time.Duration) error {
	err := c.cache.SetEndpointObservation(ctx, &pairingtypes.EndpointObservationSet{
		ChainId:      chainID,
		ApiInterface: apiInterface,
		EndpointId:   endpointID,
		PodId:        podID,
		Block:        block,
		TtlMs:        ttl.Milliseconds(),
	})
	c.warnIfUnimplemented(err)
	return err
}

func (c *cachePeerObservations) Fetch(ctx context.Context, chainID, apiInterface, endpointID string) (int64, string, time.Duration, bool, error) {
	reply, err := c.cache.GetEndpointObservation(ctx, &pairingtypes.EndpointObservationGet{
		ChainId:      chainID,
		ApiInterface: apiInterface,
		EndpointId:   endpointID,
	})
	if err != nil {
		c.warnIfUnimplemented(err)
		return 0, "", 0, false, err
	}
	if !reply.GetFound() {
		return 0, "", 0, false, nil
	}
	return reply.GetBlock(), reply.GetPodId(), time.Duration(reply.GetAgeMs()) * time.Millisecond, true, nil
}

// shouldPublishLocked reports whether to publish this pod's observation of endpointURL to the
// fleet store, and stamps the attempt when it says yes. Caller holds obsMu, and calls this ONLY for
// an observation the tip store accepted — a straggler the local store rejected would be rejected by
// the fleet store's identical monotonic rule as well, so publishing it spends a window for nothing.
//
// Throttled to one publish per half freshness window: the note only has to stay continuously
// borrowable, while a busy endpoint observes far more often than that — on Solana most served
// relays carry a tip. Half the window rather than the whole one so jitter cannot reopen the gap
// this publishing exists to close: a note published at t is borrowable until t+freshness, and the
// next publish lands at t+freshness/2.
func (m *EndpointMonitor) shouldPublishLocked(endpointURL string, at time.Time) bool {
	if m.peerObservations == nil {
		return false
	}
	if last, ok := m.lastPublished[endpointURL]; ok && at.Sub(last) < m.relayGateFreshness/2 {
		return false
	}
	m.lastPublished[endpointURL] = at
	return true
}

// publishLocalObservation pushes what this pod learned by contacting the node ITSELF — a
// successful poll or a served relay — to the fleet store, off the calling goroutine. A borrowed
// (SourcePeer) value must never reach here: republishing one refreshes the fleet note's timestamp
// without anyone having contacted the node, so a stale block would look permanently fresh to every
// pod. Skipped entirely when no store is wired.
func (m *EndpointMonitor) publishLocalObservation(endpointURL string, block int64) {
	if m.peerObservations == nil || block <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(m.ctx, peerPublishTimeout)
		defer cancel()
		err := m.peerObservations.Publish(ctx, m.chainID, m.apiInterface, EndpointID(endpointURL), m.podID, block, m.relayGateFreshness*peerObservationTTLMultiplier)
		if err != nil && m.ctx.Err() == nil {
			m.noteGateError(endpointURL, metrics.TrackerGateOpPublish)
			utils.LavaFormatDebug("fleet gate: publishing local observation failed",
				utils.LogAttr("chainID", m.chainID),
				utils.LogAttr("apiInterface", m.apiInterface),
				utils.LogAttr("endpoint", endpointURL),
				utils.LogAttr("error", err),
			)
		}
	}()
}

// localPollHealthy reports whether THIS pod's last poll of the endpoint succeeded — the gate's
// precondition. An adopted peer block refreshes ObservedAt, which is all probing.Verdict reads
// for `alive` (a failed poll moves neither it nor LatestBlock), so without this a pod that lost
// its path would read the endpoint ALIVE forever instead of disabling it after one staleness
// window. ConsecutivePollFailures does climb, but nothing reads it for liveness.
//
// No record means unhealthy: a live tracker always has one (the init fetch records before the
// poll loop starts), so absence is unexpected and costs one local poll.
func (m *EndpointMonitor) localPollHealthy(endpointURL string) bool {
	m.obsMu.RLock()
	defer m.obsMu.RUnlock()
	o, ok := m.observations[endpointURL]
	return ok && o.ConsecutivePollFailures == 0
}

// freshPeerTip is the peer half of the traffic gate. It reports true — and adopts the peer's
// block into this endpoint's tip under SourcePeer — only when this pod's own last poll of the
// endpoint succeeded, a live observation exists, it was published by ANOTHER pod, and it is
// younger than the gate freshness window (the same "~one block of staleness" horizon the relay
// gate uses). Any error or miss means "poll locally". gen is the tracker's observation
// generation (see recordPollObservation).
func (m *EndpointMonitor) freshPeerTip(endpointURL string, gen uint64, now time.Time) (int64, bool) {
	if m.peerObservations == nil {
		return 0, false
	}
	// Before the fetch: a failing pod must not spend a cache round-trip on an answer it cannot
	// use, and dropping the gate returns it to full-cadence local polling.
	if !m.localPollHealthy(endpointURL) {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(m.ctx, m.peerFetchBudget())
	defer cancel()
	block, podID, age, found, err := m.peerObservations.Fetch(ctx, m.chainID, m.apiInterface, EndpointID(endpointURL))
	if err != nil {
		if m.ctx.Err() == nil {
			m.noteGateError(endpointURL, metrics.TrackerGateOpFetch)
			utils.LavaFormatDebug("fleet gate: fetching peer observation failed, polling locally",
				utils.LogAttr("chainID", m.chainID),
				utils.LogAttr("endpoint", endpointURL),
				utils.LogAttr("error", err),
			)
		}
		return 0, false
	}
	if !found || block <= 0 || podID == m.podID || age > m.relayGateFreshness {
		return 0, false
	}
	m.recordPeerObservation(endpointURL, gen, block, now.Add(-age))
	return block, true
}

// recordPeerObservation adopts a peer pod's poll result into this endpoint's tip triple
// (Source = Peer). Like RecordRelayObservation it never touches the poll-health fields, is
// generation-gated, goes through the store's block-monotonic guard, and feeds the per-chain
// ChainState tip only when the write advanced the stored tip.
//
// It deliberately does NOT publish: this pod learned nothing first-hand. See
// publishLocalObservation.
func (m *EndpointMonitor) recordPeerObservation(endpointURL string, gen uint64, block int64, at time.Time) bool {
	if block <= 0 {
		return false
	}
	var tipBlock int64
	defer func() {
		if tipBlock > 0 && m.onTipObservation != nil {
			m.onTipObservation(tipBlock)
		}
	}()

	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	if m.stopped {
		return false
	}
	if liveGen, ok := m.generations[endpointURL]; !ok || liveGen != gen {
		return false
	}
	if _, exists := m.observations[endpointURL]; !exists {
		m.observations[endpointURL] = EndpointObservation{}
	}
	if endpointtip.Default().Set(m.tipKey(endpointURL), endpointtip.Tip{
		Block:      block,
		ObservedAt: at,
		Source:     endpointtip.SourcePeer,
	}, m.tipStaleAfter) {
		tipBlock = block
		return true
	}
	return false
}
