package rpcsmartrouter

// Bounded in-memory record of cross-validation dissent outcomes, read back by
// GET /debug/cross-validation-events (MAG-2772).
//
// The router already recorded dissent twice, and neither surface can be asserted on from an
// automated run:
//
//   - The info logs ("cross-validation outlier detected" / "cross-validation straggler resolved").
//     The team's rule is that logs are diagnostic evidence only, never a gating assertion.
//   - smartrouter_cross_validation_mismatch_total on the metrics port, which is not published in
//     the cluster. Reading it needs a hand-held port-forward, and when the tunnel is down the read
//     comes back empty and the test passes having measured nothing — the same silent pass
//     /debug/provider-scores was added to end (MAG-2707).
//
// The counter also cannot answer what the parked tests ask. It carries {spec, apiInterface, method,
// group, finality} and nothing else: no provider name, no request id. A test that needs "which
// provider dissented on THIS request" gets no answer from a counter, which is why this surface is
// event-shaped. Counts are derivable from events; the reverse is not.
//
// Recording is DEBUG-MODE ONLY. The ring is installed by enableCrossValidationEventRing, called
// from Start alongside utils.EnableDebugLogBuffer when --debug-address is set, so a production
// router pays one atomic nil load per dissent and stores nothing.

import (
	"context"
	"encoding/hex"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/utils"
)

// crossValidationEventRingCapacity bounds the ring installed in debug mode. Events are recorded per
// DISSENT and per STRAGGLER RESOLUTION, not per request, so this is a much slower-filling ring than
// the 50k-line log buffer; 4096 covers a long nightly run and caps the store at roughly a megabyte.
const crossValidationEventRingCapacity = 4096

// Which recording path produced an event. The automation needs to tell the two apart: a dissent
// compared BEFORE the reply was received in time to be in smartrouter-cross-validation-disagreeing-providers,
// while a straggler resolved AFTER the reply was not (it was in pending-providers instead).
const (
	crossValidationEventSourceReplyTime = "reply-time"
	crossValidationEventSourceStraggler = "straggler"
)

// crossValidationEvent is one recorded cross-validation comparison outcome.
//
// Field names are the Go identifiers the other /debug row endpoints use, and every row carries its
// own ChainID + ApiInterface so a multi-chain router's rows stay self-describing.
type crossValidationEvent struct {
	// Seq is a monotonic per-process counter, assigned under the ring lock. RecordedAt is RFC3339
	// (the /debug convention) and therefore second-granular, so Seq — not the timestamp — is the
	// unambiguous ordering and dedup key.
	Seq        uint64
	RecordedAt time.Time

	Source       string // crossValidationEventSourceReplyTime | crossValidationEventSourceStraggler
	ChainID      string
	ApiInterface string

	// RequestID is the smart-router-guid: the value the router returns in the Smart-Router-Guid response header,
	// formatted the same way (decimal). NOTE this is NOT /debug/logs' request_id, which is the
	// caller's X-Request-Id header — a different identifier that this router does not require.
	RequestID string
	Method    string

	ProviderAddress string
	ProviderGroup   string // already folded to common.DefaultProviderGroup when the provider carries no label

	// Outcome is "disagreed" for a reply-time content outlier, and the straggler outcome
	// (agreed / disagreed / node-error / protocol-error / not-received) for the async path.
	Outcome  string
	Finality string // finalized | not_finalized | unknown

	// ConsensusHash is the hash the quorum agreed on; OutlierHash is this provider's OWN response
	// hash — the outlier hash on a dissent, and deliberately the same field on an agreed straggler,
	// where it equals ConsensusHash and is exactly what the positive control asserts. Both are the
	// full 32-byte SHA256 in hex, so the short form in the logs is a prefix of them and a log line
	// and a row correlate. OutlierHash is the zero hash (rendered as "") for the outcomes with no
	// content to hash: node-error, protocol-error and not-received.
	ConsensusHash [32]byte
	OutlierHash   [32]byte

	// MismatchCounted reports whether THIS event is the one that incremented
	// smartrouter_cross_validation_mismatch_total. The counter's contract is once per distinct
	// outlier group per request, so a second dissent from an already-counted group records an event
	// with MismatchCounted=false. Without this the dedup rule is invisible to a reader that only
	// sees the events.
	MismatchCounted bool

	// DelayMs is how long after the reply a straggler resolved. 0 on the reply-time path, which by
	// definition resolved before the reply.
	DelayMs int64
}

// crossValidationEventRing is the bounded store: append at the tail, evict the oldest at capacity.
// Written from the request goroutine (reply-time path) and from each request's straggler-watcher
// goroutine, read from the debug HTTP handler — so every access takes the lock.
type crossValidationEventRing struct {
	mu       sync.Mutex
	buf      []crossValidationEvent
	capacity int
	nextSeq  uint64
	dropped  uint64 // evicted since the ring was installed, so truncation is never silent
}

// crossValidationEventRecorder holds the live ring, or nil when recording is off (the production
// default). An atomic pointer because enableCrossValidationEventRing runs during Start while relay
// goroutines may already be recording.
var crossValidationEventRecorder atomic.Pointer[crossValidationEventRing]

// enableCrossValidationEventRing installs a fresh ring. Debug-mode only — called from
// rpcsmartrouter.Start when --debug-address is set, next to utils.EnableDebugLogBuffer, so the two
// debug-only stores are switched on by the same condition in the same place.
func enableCrossValidationEventRing(capacity int) {
	if capacity <= 0 {
		capacity = crossValidationEventRingCapacity
	}
	crossValidationEventRecorder.Store(&crossValidationEventRing{
		buf:      make([]crossValidationEvent, 0, capacity),
		capacity: capacity,
	})
}

// crossValidationEventRecordingEnabled reports whether a ring is installed. The handler answers 503
// rather than an empty 200 when it is not: "the recorder was never switched on" and "the recorder
// was on and saw no dissent" are opposite answers, and only one of them means a test measured
// something.
func crossValidationEventRecordingEnabled() bool {
	return crossValidationEventRecorder.Load() != nil
}

// recordCrossValidationEvent stores one event, or does nothing when recording is off. Seq and
// RecordedAt are assigned here so callers cannot get them wrong.
func recordCrossValidationEvent(event crossValidationEvent) {
	ring := crossValidationEventRecorder.Load()
	if ring == nil {
		return
	}
	ring.mu.Lock()
	defer ring.mu.Unlock()
	ring.nextSeq++
	event.Seq = ring.nextSeq
	event.RecordedAt = time.Now()
	if len(ring.buf) >= ring.capacity {
		ring.buf = ring.buf[1:]
		ring.dropped++
	}
	ring.buf = append(ring.buf, event)
}

// crossValidationEventFilter narrows a read. Every field is optional; the set ones are ANDed.
type crossValidationEventFilter struct {
	RequestID string // the smart-router-guid, exact match
	ChainID   string // exact match, case-sensitive like the other debug chain filters
	Outcome   string // exact match
	Limit     int    // keep the most recent N after filtering; <= 0 means the whole ring
}

// readCrossValidationEvents returns the matching events oldest-first, along with the number evicted
// since the ring was installed. enabled is false when no ring is installed, which the caller must
// distinguish from an empty result.
func readCrossValidationEvents(filter crossValidationEventFilter) (events []crossValidationEvent, dropped uint64, enabled bool) {
	ring := crossValidationEventRecorder.Load()
	if ring == nil {
		return nil, 0, false
	}
	ring.mu.Lock()
	defer ring.mu.Unlock()

	matched := make([]crossValidationEvent, 0, len(ring.buf))
	for _, event := range ring.buf {
		if filter.RequestID != "" && event.RequestID != filter.RequestID {
			continue
		}
		if filter.ChainID != "" && event.ChainID != filter.ChainID {
			continue
		}
		if filter.Outcome != "" && event.Outcome != filter.Outcome {
			continue
		}
		matched = append(matched, event)
	}
	// Tail, like /debug/logs: a limit keeps the most RECENT matches, since a test reading back its
	// own request wants what just happened, not the oldest thing still in the ring.
	if filter.Limit > 0 && len(matched) > filter.Limit {
		matched = matched[len(matched)-filter.Limit:]
	}
	return matched, ring.dropped, true
}

// clearCrossValidationEvents drops every recorded event and resets the eviction count, so a test can
// isolate itself from earlier traffic. Seq deliberately keeps counting: it identifies an event within
// the process, and restarting it would let a cleared-then-refilled ring hand out ids a caller already
// holds. Returns the number of events dropped and whether a ring was installed at all.
func clearCrossValidationEvents() (cleared int, enabled bool) {
	ring := crossValidationEventRecorder.Load()
	if ring == nil {
		return 0, false
	}
	ring.mu.Lock()
	defer ring.mu.Unlock()
	cleared = len(ring.buf)
	ring.buf = ring.buf[:0]
	ring.dropped = 0
	return cleared, true
}

// crossValidationRequestID renders the request's smart-router-guid the same way the Smart-Router-Guid response
// header does (decimal), so the value a test reads off its own response is the value it filters
// with. Empty when the context carries no guid — the recording paths always run under a relay
// context that has one, so an empty value means an unwired test fixture, not a lost request.
func crossValidationRequestID(ctx context.Context) string {
	guid, found := utils.GetUniqueIdentifier(ctx)
	if !found || guid == 0 {
		return ""
	}
	return strconv.FormatUint(guid, 10)
}

// crossValidationHashHex renders a content hash for the wire: full 32-byte hex, and "" for the zero
// hash. The zero value is a real sentinel here — it means "no content was hashed" (a node error, a
// protocol error, or a straggler that never answered) — and rendering it as 64 zeros would read as a
// hash that merely differs from the consensus.
func crossValidationHashHex(hash [32]byte) string {
	if hash == ([32]byte{}) {
		return ""
	}
	return hex.EncodeToString(hash[:])
}

// crossValidationEventRow flattens an event into the self-describing row shape the other /debug
// state endpoints return.
func crossValidationEventRow(event crossValidationEvent) map[string]any {
	return map[string]any{
		"Seq":             event.Seq,
		"RecordedAt":      debugTimeRFC3339(event.RecordedAt),
		"Source":          event.Source,
		"ChainID":         event.ChainID,
		"ApiInterface":    event.ApiInterface,
		"RequestID":       event.RequestID,
		"Method":          event.Method,
		"ProviderAddress": event.ProviderAddress,
		"ProviderGroup":   event.ProviderGroup,
		"Outcome":         event.Outcome,
		"Finality":        event.Finality,
		"ConsensusHash":   crossValidationHashHex(event.ConsensusHash),
		"OutlierHash":     crossValidationHashHex(event.OutlierHash),
		"MismatchCounted": event.MismatchCounted,
		"DelayMs":         event.DelayMs,
	}
}
