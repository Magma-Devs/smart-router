package rpcsmartrouter

// Structured, append-only compliance record for every cross-validated request (MAG-2787), part of
// MAG-2782's Kraken compliance work (RTS (EU) 2024/1774 Art. 14(1)(a)/23(2)(b), DORA Art. 12(7)).
//
// This is a log-format change, not a new data store: the router has no read API for these records
// (that is MAG-2788) and holds nothing beyond the async write queue below. The record carries
// digests and metadata only, never a request or response body — bodies can carry personal data, and
// under MiCA Art. 87(1)(b) a pending client order is inside information.
//
// Digest-ambiguity fix first: a zero [32]byte hash means three different things elsewhere in this
// package — an empty-but-valid payload, a field that was never hashed (a node/protocol error), and
// the deliberate nil-fallback consensus sentinel (see crossValidationHashHex in
// cross_validation_events.go, which already renders all three as "" rather than as a differing
// hash). Every digest field below goes through that same helper, and — critically — is always read
// alongside an explicit Outcome, so a record reader can never confuse "answered with empty content"
// (Outcome agreed/disagreed, Digest "") with "never answered" (Outcome node-error/protocol-error/
// not-received, Digest always ""). On a failed quorum there is no served answer, so ServedDigest is
// left "" explicitly rather than inherited from a zero consensus hash.
//
// Writes are asynchronous and must never fail a relay: emitCrossValidationRecord only ever does a
// non-blocking channel send. A full queue drops the record and increments
// smartrouter_cross_validation_record_dropped_total rather than blocking the request goroutine; the
// same drop is independently visible as a hole in the Seq sequence within the record stream itself,
// so a dropped record is never silent even to a reader with no access to /metrics.
//
// Emission is deliberately NOT routed through utils.FormatInfo: that family's level gate means a
// router operated at --log-level warn (or above) would silently stop producing compliance evidence,
// which is unacceptable for a record whose entire purpose is regulatory evidence. crossValidationRecordLogger
// below is a dedicated, fixed-level logger that writes to the same stream (stderr, in the same
// JSON-vs-console format the rest of the process uses) so it still flows into the existing log
// pipeline the router already ships to Loki on S3, but can never be silenced by the operator's
// ordinary verbosity knob.
import (
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	// crossValidationRecordDigestAlgorithmVersion stamps the canonicalization behind every digest in
	// this record: decode the reply with json.Decoder.UseNumber(), re-marshal with encoding/json, then
	// SHA256 the result (relaycore's canonicalResponseHash). This is Go's own marshal behavior, not a
	// standard canonical-JSON form (e.g. JCS/RFC 8785) — bump this version if that canonicalization is
	// ever rewritten, or a Go stdlib marshal change silently makes every earlier record uninterpretable.
	crossValidationRecordDigestAlgorithmVersion = 1

	// crossValidationRecordChannelCapacity bounds the async write queue. Sized well above any
	// plausible burst of concurrent cross-validated requests on one process; a full queue means the
	// log sink itself is the bottleneck, which no queue size fixes, so growing this further would
	// only delay the drop, not prevent it.
	crossValidationRecordChannelCapacity = 1024

	// crossValidationRecordTenantMaxLength bounds the tenant field so a free-text customer-id header
	// cannot grow the record body unboundedly. Mirrors metrics.customer_label.go's bound on the same
	// underlying value (ProjectHash) for its Prometheus "customer" label, for the same reason: a
	// value this is derived from is caller-controlled.
	crossValidationRecordTenantMaxLength = 128
)

// crossValidationEndpointOutcome is one provider's part in a record: either how it answered at reply
// time, or how a straggler resolved afterward. DelayMs is 0 (omitted) for every reply-time entry,
// since by definition those resolved before the reply.
type crossValidationEndpointOutcome struct {
	ProviderAddress string `json:"providerAddress"`
	ProviderGroup   string `json:"providerGroup,omitempty"`
	// Outcome is one of common.CrossValidationStragglerOutcome* (agreed, disagreed, node-error,
	// protocol-error, not-received) — the same closed enum the straggler path already uses, so a
	// reply-time dissent and a late one are described identically.
	Outcome string `json:"outcome"`
	// Digest is "" whenever there was no content to hash: see the digest-ambiguity note at the top of
	// this file. Always read together with Outcome, never alone.
	Digest  string `json:"digest,omitempty"`
	DelayMs int64  `json:"delayMs,omitempty"`
}

// crossValidationRecord is the structured record MAG-2787 requires: exactly one per
// cross-validated request, success or failure, carrying the policy as resolved for that request —
// not what the config says now.
type crossValidationRecord struct {
	Time time.Time `json:"time"`
	// Seq is a monotonic per-process counter, assigned when the record is first built — before it is
	// known whether the write queue will have room — so a dropped record still spends its slot and
	// leaves a visible gap in the sequence the next successfully-written record carries.
	Seq       uint64 `json:"seq"`
	RequestID string `json:"requestId"`
	// Tenant identifies the caller for this record (the customer-id/dapp-id header, bounded the same
	// way the "customer" Prometheus label is — see crossValidationRecordTenantMaxLength).
	Tenant       string `json:"tenant,omitempty"`
	ChainID      string `json:"chainId"`
	ApiInterface string `json:"apiInterface"`
	Method       string `json:"method"`
	BlockHeight  int64  `json:"blockHeight"`
	Finality     string `json:"finality"`

	// AgreementThreshold/MaxParticipants/MinGroups are the policy as RESOLVED for this request (after
	// clamp(caller, floor, cap)), not whatever the config says when the record is later read.
	AgreementThreshold int `json:"agreementThreshold"`
	MaxParticipants    int `json:"maxParticipants"`
	MinGroups          int `json:"minGroups"`

	Status        string `json:"status"` // success | failed
	FailureReason string `json:"failureReason,omitempty"`

	// ServedDigest is which answer was served — "" on failure, where there was no winner (see the
	// digest-ambiguity note above; this is never a stand-in zero hash).
	ServedDigest string `json:"servedDigest,omitempty"`
	// PluralitySize/PluralityProviders are the largest agreeing group's composition on a FAILED quorum
	// (relaycore.CrossValidationPlurality, the same computation the response's plurality headers use),
	// so a failure record still names who the odd one out was.
	PluralitySize      int      `json:"pluralitySize,omitempty"`
	PluralityProviders []string `json:"pluralityProviders,omitempty"`

	DigestAlgorithmVersion int `json:"digestAlgorithmVersion"`

	// Endpoints covers every provider queried for this request, including ones never heard from
	// (not-received). LateResolutions is the subset of Endpoints that resolved after the reply went
	// out — present so "how they resolved" survives even though Endpoints already carries their final
	// outcome; empty whenever nothing was still pending at reply time.
	Endpoints       []crossValidationEndpointOutcome `json:"endpoints"`
	LateResolutions []crossValidationEndpointOutcome `json:"lateResolutions,omitempty"`
}

// crossValidationRecordSeq is the process-wide monotonic sequence counter. Package-level and shared
// across every chain/apiInterface this process serves — a single, unbroken sequence is what makes a
// gap from a dropped record visible without needing per-chain bookkeeping.
var crossValidationRecordSeq atomic.Uint64

func nextCrossValidationRecordSeq() uint64 {
	return crossValidationRecordSeq.Add(1)
}

// crossValidationRecordInputs bundles what building a record needs. The two call sites (reply time,
// in appendHeadersToRelayResult; straggler resolution, in watchCrossValidationStragglers) each
// gather these from values they already hold locally.
type crossValidationRecordInputs struct {
	ChainID      string
	ApiInterface string
	Method       string
	RequestID    string
	Tenant       string
	BlockHeight  int64
	Finality     string

	CVParams *common.CrossValidationParams

	CVSuccess     bool
	FailureReason string
	ConsensusHash [32]byte

	SuccessResults       []common.RelayResult
	NodeErrorResults     []common.RelayResult
	ProtocolErrorResults []relaycore.RelayError
	// AllProviders is every provider queried for this request. Anything in here not present in the
	// three result slices above is reported not-received.
	AllProviders []string
}

// buildCrossValidationRecord assembles one record from a complete view of a request's outcome. It
// assigns Seq, so call it exactly once per request (see the two call sites' guard conditions, which
// partition "no stragglers were pending" from "defer to the straggler watcher" so neither builds a
// record for a request the other already owns).
func buildCrossValidationRecord(in crossValidationRecordInputs) *crossValidationRecord {
	rec := &crossValidationRecord{
		Time:                   time.Now().UTC(),
		Seq:                    nextCrossValidationRecordSeq(),
		RequestID:              in.RequestID,
		Tenant:                 in.Tenant,
		ChainID:                in.ChainID,
		ApiInterface:           in.ApiInterface,
		Method:                 in.Method,
		BlockHeight:            in.BlockHeight,
		Finality:               in.Finality,
		DigestAlgorithmVersion: crossValidationRecordDigestAlgorithmVersion,
	}
	if in.CVParams != nil {
		rec.AgreementThreshold = in.CVParams.AgreementThreshold
		rec.MaxParticipants = in.CVParams.MaxParticipants
		rec.MinGroups = in.CVParams.MinGroups
	}
	if in.CVSuccess {
		rec.Status = "success"
		rec.ServedDigest = crossValidationHashHex(in.ConsensusHash)
	} else {
		rec.Status = "failed"
		rec.FailureReason = in.FailureReason
		rec.PluralitySize, rec.PluralityProviders = relaycore.CrossValidationPlurality(in.SuccessResults)
	}

	accounted := make(map[string]struct{}, len(in.SuccessResults)+len(in.NodeErrorResults)+len(in.ProtocolErrorResults))
	for _, result := range in.SuccessResults {
		addr := result.ProviderInfo.ProviderAddress
		if addr == "" {
			continue
		}
		accounted[addr] = struct{}{}
		outcome := common.CrossValidationStragglerOutcomeDisagreed
		if in.CVSuccess && result.ResponseHash == in.ConsensusHash {
			outcome = common.CrossValidationStragglerOutcomeAgreed
		}
		rec.Endpoints = append(rec.Endpoints, crossValidationEndpointOutcome{
			ProviderAddress: addr,
			ProviderGroup:   crossValidationRecordGroup(result.ProviderInfo.ProviderGroup),
			Outcome:         outcome,
			Digest:          crossValidationHashHex(result.ResponseHash),
		})
	}
	for _, result := range in.NodeErrorResults {
		addr := result.ProviderInfo.ProviderAddress
		if addr == "" {
			continue
		}
		accounted[addr] = struct{}{}
		rec.Endpoints = append(rec.Endpoints, crossValidationEndpointOutcome{
			ProviderAddress: addr,
			ProviderGroup:   crossValidationRecordGroup(result.ProviderInfo.ProviderGroup),
			Outcome:         common.CrossValidationStragglerOutcomeNodeError,
		})
	}
	for _, relayErr := range in.ProtocolErrorResults {
		addr := relayErr.ProviderInfo.ProviderAddress
		if addr == "" {
			continue
		}
		accounted[addr] = struct{}{}
		rec.Endpoints = append(rec.Endpoints, crossValidationEndpointOutcome{
			ProviderAddress: addr,
			ProviderGroup:   crossValidationRecordGroup(relayErr.ProviderInfo.ProviderGroup),
			Outcome:         common.CrossValidationStragglerOutcomeProtocolError,
		})
	}
	for _, addr := range in.AllProviders {
		if addr == "" {
			continue
		}
		if _, ok := accounted[addr]; ok {
			continue
		}
		rec.Endpoints = append(rec.Endpoints, crossValidationEndpointOutcome{
			ProviderAddress: addr,
			Outcome:         common.CrossValidationStragglerOutcomeNotReceived,
		})
	}
	return rec
}

func crossValidationRecordGroup(group string) string {
	if group == "" {
		return common.DefaultProviderGroup
	}
	return group
}

// patchCrossValidationRecordStraggler updates a record in place once a straggler resolves: flips its
// Endpoints entry from the provisional not-received outcome to the real one, and appends the same
// resolution to LateResolutions so "how it resolved" survives even though Endpoints only keeps the
// final state. Safe without a lock: called only from the single straggler-watcher goroutine, strictly
// before the record is hexposed to emitCrossValidationRecord (see watchCrossValidationStragglers).
func patchCrossValidationRecordStraggler(rec *crossValidationRecord, straggler relaycore.CrossValidationStragglerResult) {
	resolved := crossValidationEndpointOutcome{
		ProviderAddress: straggler.ProviderAddress,
		ProviderGroup:   crossValidationRecordGroup(straggler.ProviderGroup),
		Outcome:         straggler.Outcome,
		Digest:          crossValidationHashHex(straggler.ResponseHash),
		DelayMs:         straggler.Delay.Milliseconds(),
	}
	for i := range rec.Endpoints {
		if rec.Endpoints[i].ProviderAddress == straggler.ProviderAddress {
			rec.Endpoints[i] = resolved
			rec.Endpoints[i].DelayMs = 0 // the final state, not a late-resolution diagnostic
			break
		}
	}
	rec.LateResolutions = append(rec.LateResolutions, resolved)
}

// crossValidationRecordTenant renders the bounded tenant identifier for a record from the request's
// customer-id/dapp-id header value (metrics.RelayMetrics.ProjectHash — the same value the "customer"
// Prometheus label is normalized from). Mirrors metrics.customer_label.go's length bound without
// depending on its per-spec admission registry, which exists to bound Prometheus series growth, a
// concern a log body field does not have.
func crossValidationRecordTenant(analytics *metrics.RelayMetrics) string {
	if analytics == nil || analytics.ProjectHash == "" {
		return metrics.CustomerNone
	}
	customer := analytics.ProjectHash
	if len(customer) > crossValidationRecordTenantMaxLength || !utf8.ValidString(customer) {
		return metrics.CustomerOther
	}
	return customer
}

// crossValidationRecordChan is the async write queue. Unbuffered sends would put log I/O on the
// relay's hot path; emitCrossValidationRecord only ever does a non-blocking send into this, so a slow
// or unavailable log pipeline can never fail a relay (MAG-2787's rule).
var crossValidationRecordChan = make(chan *crossValidationRecord, crossValidationRecordChannelCapacity)

// crossValidationRecordWriterStart lazily starts the single consumer goroutine the first time a
// record is actually emitted, so a process that never exercises cross-validation never pays for an
// idle goroutine. sync.OnceFunc makes the start itself safe under concurrent first calls.
var crossValidationRecordWriterStart = sync.OnceFunc(func() {
	go func() {
		for rec := range crossValidationRecordChan {
			writeCrossValidationRecord(rec)
		}
	}()
})

// crossValidationRecordLogger is a dedicated logger fixed at Info level, independent of the
// operator's --log-level: see the file-level doc comment for why a compliance record must not be
// silenceable by that unrelated operational knob. Writes to stderr in the same format
// (JSON in production, console otherwise) the rest of the process uses, so the line still flows into
// whatever already scrapes the process's existing output into Loki.
var crossValidationRecordLogger = newCrossValidationRecordLogger()

func newCrossValidationRecordLogger() zerolog.Logger {
	if utils.JsonFormat {
		return zerolog.New(os.Stderr).Level(zerolog.InfoLevel).With().Timestamp().Logger()
	}
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true}).Level(zerolog.InfoLevel).With().Timestamp().Logger()
}

func writeCrossValidationRecord(rec *crossValidationRecord) {
	crossValidationRecordLogger.Info().Interface("crossValidationRecord", rec).Msg("cross-validation record")
}

// crossValidationRecordObserveForTest, when non-nil, is called with every record passed to
// emitCrossValidationRecord — including ones later dropped by a full queue — so tests can assert on
// a record's full content without parsing the log line it eventually becomes. Production code never
// sets this; tests must restore it to nil (t.Cleanup) since it is process-global.
var crossValidationRecordObserveForTest func(*crossValidationRecord)

// emitCrossValidationRecord hands a finished record to the async writer, or drops it and counts the
// drop when the queue is full. Never blocks: a full queue is a log-pipeline capacity problem no
// amount of relay-goroutine waiting would fix, and this call must never be the reason a relay fails.
func (rpcss *RPCSmartRouterServer) emitCrossValidationRecord(rec *crossValidationRecord) {
	if rec == nil {
		return
	}
	if observe := crossValidationRecordObserveForTest; observe != nil {
		observe(rec)
	}
	crossValidationRecordWriterStart()
	select {
	case crossValidationRecordChan <- rec:
	default:
		if rpcss.smartRouterEndpointMetrics != nil {
			rpcss.smartRouterEndpointMetrics.SetCrossValidationRecordDroppedMetric(rec.ChainID, rec.ApiInterface)
		}
	}
}
