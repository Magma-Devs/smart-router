package relaycore

// Bounded in-memory record of the answers each request's attempts handed back, in the order they
// reached the request, read back by GET /debug/relay-answers (MAG-4358).
//
// A write goes to every primary at once, and what its caller gets depends on which answers reach
// the router and in which order: a refusal that arrives before an acceptance is held while the
// router waits, and between refusals of equal rank the first to arrive is kept. A test that stages
// that order (one upstream refuses at once, another accepts later) had no read showing the order
// actually happened. The log may not be asserted on, the simulator's history says when a request
// reached a provider and not when its answer reached the router, and the cross-validation events
// cover cross-validated requests only.
//
// An answer is recorded when an attempt hands it to its request (SetResponse), not when the router
// reads it (handleResponse). A write returns on the first acceptance, and the deliveries still
// running then hand their answers to a request that reads nothing more (statefulDeliveryLinger).
// Recorded at the read, those answers would be missing, and a write to three providers would show
// two. The verdict is filled in at the read, so an answer the router never read before it replied
// keeps RelayAnswerUnread.
//
// Recording is DEBUG-MODE ONLY, like the cross-validation event ring: rpcsmartrouter.Start installs
// the ring when --debug-address is set. With none installed, an answer costs one atomic nil load.

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// RelayAnswerRingCapacity bounds the ring a debug-mode router installs. A test reads its own
// request back right after the reply, so the ring only has to outlast the traffic in between. The
// cross-validation ring's 4096 holds the answers of the last thousand requests or more, three to a
// write, in about a megabyte.
const RelayAnswerRingCapacity = 4096

// What the router made of an answer when it read it, and the value of one it did not read.
const (
	RelayAnswerSuccess       = "success"        // filed as a result
	RelayAnswerNodeError     = "node-error"     // the node answered, with an error
	RelayAnswerProtocolError = "protocol-error" // the attempt failed: no answer, or a status the transport refuses (a 5xx, a 429)
	RelayAnswerUnread        = "unread"         // handed over after the router stopped reading for the reply
)

// RelayAnswer is one attempt's answer as it reached its request.
type RelayAnswer struct {
	// Seq is a per-process counter, taken as the attempt hands its answer to the request, just
	// before the answer joins the FIFO queue the request reads. So Seq is also the order the router
	// read the answers in, except that two answers handed over in the same instant can swap places
	// between the counter and the queue. ArrivedAt is the wall-clock time of the hand-over and
	// Elapsed its time since the request's relay processing started.
	Seq       uint64
	ArrivedAt time.Time
	Elapsed   time.Duration

	ChainID      string
	ApiInterface string
	// RequestID is the request's guid in decimal: the value of the Lava-Guid reply header.
	RequestID string
	Method    string

	ProviderAddress string
	// StatusCode is the status the upstream answered with: the reply's, or for a failed attempt the
	// one its HTTPStatusError carries (relayInnerDirect keeps an error status off the result, so the
	// client is not handed it). 0 when no status came back. On gRPC it is the call's code, where 0 is
	// OK.
	StatusCode int
	Outcome    string
}

// relayAnswerRing is the bounded store: append at the tail, evict the oldest at capacity. Written
// from every attempt's goroutine and from the request's reader, read from the debug handler, so
// every access takes the lock.
type relayAnswerRing struct {
	mu       sync.Mutex
	buf      []RelayAnswer
	capacity int
	nextSeq  uint64
	dropped  uint64 // evicted since the ring was installed, so truncation is never silent
}

// relayAnswerRecorder holds the live ring, or nil when recording is off (the production default).
var relayAnswerRecorder atomic.Pointer[relayAnswerRing]

// EnableRelayAnswerRecording installs a fresh ring. Debug-mode only: rpcsmartrouter.Start calls it
// when --debug-address is set.
func EnableRelayAnswerRecording(capacity int) {
	if capacity <= 0 {
		capacity = RelayAnswerRingCapacity
	}
	relayAnswerRecorder.Store(&relayAnswerRing{buf: make([]RelayAnswer, 0, capacity), capacity: capacity})
}

// DisableRelayAnswerRecording removes the ring. A router never calls it; a test does, to leave the
// process-wide recorder as it found it.
func DisableRelayAnswerRecording() {
	relayAnswerRecorder.Store(nil)
}

// ReadRelayAnswers returns the answers of one request, or of every request when requestID is empty,
// oldest first, and how many answers the ring evicted since it was installed. A limit above zero
// keeps the most recent matches. enabled is false when no ring is installed, which the caller must
// tell apart from an empty result.
func ReadRelayAnswers(requestID string, limit int) (answers []RelayAnswer, dropped uint64, enabled bool) {
	ring := relayAnswerRecorder.Load()
	if ring == nil {
		return nil, 0, false
	}
	ring.mu.Lock()
	defer ring.mu.Unlock()
	answers = make([]RelayAnswer, 0, len(ring.buf))
	for _, answer := range ring.buf {
		if requestID == "" || answer.RequestID == requestID {
			answers = append(answers, answer)
		}
	}
	if limit > 0 && len(answers) > limit {
		answers = answers[len(answers)-limit:]
	}
	return answers, ring.dropped, true
}

func (ring *relayAnswerRing) record(answer RelayAnswer) uint64 {
	ring.mu.Lock()
	defer ring.mu.Unlock()
	ring.nextSeq++
	answer.Seq = ring.nextSeq
	answer.Outcome = RelayAnswerUnread
	if len(ring.buf) >= ring.capacity {
		ring.buf = ring.buf[1:]
		ring.dropped++
	}
	ring.buf = append(ring.buf, answer)
	return answer.Seq
}

// setOutcome records the router's verdict on the answer with this Seq. The ring holds consecutive
// Seqs, so the answer's place is its distance from the oldest; an answer already evicted is skipped.
func (ring *relayAnswerRing) setOutcome(seq uint64, outcome string) {
	ring.mu.Lock()
	defer ring.mu.Unlock()
	if len(ring.buf) == 0 || seq < ring.buf[0].Seq {
		return
	}
	if i := seq - ring.buf[0].Seq; i < uint64(len(ring.buf)) && ring.buf[i].Seq == seq {
		ring.buf[i].Outcome = outcome
	}
}

// recordAnswer files the answer on the ring, when one is installed, before it joins the queue the
// request reads. The ring the answer went to rides on the response, so the verdict lands on that
// ring even if a test has installed another one since.
func (rp *RelayProcessor) recordAnswer(response *RelayResponse) {
	ring := relayAnswerRecorder.Load()
	if ring == nil {
		return
	}
	provider := response.RelayResult.ProviderInfo.ProviderAddress
	// A cache hit names no provider: no attempt answered it. The router's own health check is not a
	// client's request.
	if provider == "" || rp.GetUsedProviders().IsRouterHealthCheck() {
		return
	}
	requestID := ""
	if rp.guid != 0 {
		requestID = strconv.FormatUint(rp.guid, 10)
	}
	chainID, apiInterface := rp.chainIdAndApiInterfaceGetter.GetChainIdAndApiInterface()
	now := time.Now()
	response.answerRing = ring
	response.answerSeq = ring.record(RelayAnswer{
		ArrivedAt:       now,
		Elapsed:         now.Sub(rp.startedAt),
		ChainID:         chainID,
		ApiInterface:    apiInterface,
		RequestID:       requestID,
		Method:          rp.RelayStateMachine.GetProtocolMessage().GetApi().Name,
		ProviderAddress: provider,
		StatusCode:      answerStatus(response),
	})
}

// recordAnswerOutcome records what handleResponse made of a recorded answer.
func recordAnswerOutcome(response *RelayResponse, nodeError error) {
	if response.answerRing == nil {
		return
	}
	outcome := RelayAnswerSuccess
	switch {
	case response.Err != nil:
		outcome = RelayAnswerProtocolError
	case nodeError != nil:
		outcome = RelayAnswerNodeError
	}
	response.answerRing.setOutcome(response.answerSeq, outcome)
}

func answerStatus(response *RelayResponse) int {
	if response.RelayResult.StatusCode != 0 {
		return response.RelayResult.StatusCode
	}
	var httpErr *routersession.HTTPStatusError
	if errors.As(response.Err, &httpErr) {
		return httpErr.StatusCode
	}
	return 0
}
