package metrics

import (
	"sort"
	"strings"
	"sync"
)

const (
	// batchMethodSeparator mirrors chainlib's SEP — the token batch parsing uses to
	// join every sub-method into one synthetic api name (protocol/chainlib/jsonRPC.go,
	// protocol/chainlib/tendermintRPC.go). It is redeclared here rather than imported
	// because chainlib already imports this package; importing back would be a cycle.
	batchMethodSeparator = "&"

	// BatchMethodPrefix namespaces every collapsed batch label value so dashboards can
	// separate batch traffic from single-method traffic with a prefix match
	// (method=~"batch:.*") rather than an allowlist of method names.
	BatchMethodPrefix = "batch:"

	// BatchMethodOther is the overflow bucket, emitted when a batch's signature is past
	// the per-spec cap or spans more distinct methods than maxBatchSignatureElements.
	// It is what makes the `method` label bounded no matter what a client sends.
	BatchMethodOther = BatchMethodPrefix + "other"

	// batchSignatureElementSeparator joins sub-methods inside a signature. It is
	// deliberately NOT batchMethodSeparator: that is what keeps normalization
	// idempotent, since an already-collapsed value then carries no separator.
	batchSignatureElementSeparator = "+"

	// maxBatchSignaturesPerSpec caps how many distinct signatures one spec may
	// register. Distinct method SETS are 2^N in theory but a handful in practice —
	// batch shapes are emitted by customer code, not drawn at random. The cap is the
	// backstop for when that assumption is wrong, and
	// smartrouter_batch_signature_overflow_total reports when it binds.
	maxBatchSignaturesPerSpec = 64

	// maxBatchSignatureElements caps how many distinct methods a single signature may
	// name, so one pathological batch can't mint a kilobyte-long label value.
	maxBatchSignatureElements = 8
)

// Reasons for the smartrouter_batch_signature_overflow_total `reason` label.
const (
	batchOverflowReasonCap  = "cap"
	batchOverflowReasonWide = "wide"
)

// buildBatchSignature turns a raw joined batch api name into an order-invariant and
// repetition-invariant signature: the sorted SET of its sub-methods.
//
// Dropping order and repetition is the entire point. They are what made the raw name
// unbounded — order contributes permutations, repetition contributes length variants —
// while the set is what a client's code actually determines. So
//
//	eth_call&eth_call&…&eth_call&eth_getBalance  →  batch:eth_call+eth_getBalance
//
// collapses thousands of observed series onto one, and the magnitude that repetition
// used to (uselessly) encode is recovered from the smartrouter_batch_size histogram.
//
// Sub-methods need no allowlist check: chainlib resolves every element against the spec
// through getSupportedApi and hard-fails the parse before any batch name is built, so a
// name reaching here only ever contains spec-declared api names, never raw client input.
//
// size is the batch's element count including repeats. tooWide reports a batch spanning
// more than maxBatchSignatureElements distinct methods, for which the caller emits
// BatchMethodOther instead of an unwieldy label value.
func buildBatchSignature(apiName string) (signature string, size int, tooWide bool) {
	parts := strings.Split(apiName, batchMethodSeparator)
	seen := make(map[string]struct{}, len(parts))
	distinct := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		size++
		if _, duplicate := seen[part]; duplicate {
			continue
		}
		seen[part] = struct{}{}
		distinct = append(distinct, part)
	}

	if len(distinct) == 0 || len(distinct) > maxBatchSignatureElements {
		return BatchMethodOther, size, true
	}

	sort.Strings(distinct)
	return BatchMethodPrefix + strings.Join(distinct, batchSignatureElementSeparator), size, false
}

// batchSignatureRegistry bounds a set of label values, tracked per spec so a chatty
// chain can't consume another chain's budget. Batch signatures were the first user;
// unmatched-API names (default_method_label.go) reuse it with their own cap.
type batchSignatureRegistry struct {
	lock     sync.RWMutex
	cap      int
	admitted map[string]map[string]struct{} // spec -> set of admitted values
}

func newBatchSignatureRegistry() *batchSignatureRegistry {
	return &batchSignatureRegistry{cap: maxBatchSignaturesPerSpec, admitted: make(map[string]map[string]struct{})}
}

func newDefaultMethodRegistry() *batchSignatureRegistry {
	return &batchSignatureRegistry{cap: maxDefaultMethodsPerSpec, admitted: make(map[string]map[string]struct{})}
}

// admit reports whether signature may be used as a label value for spec.
//
// Admission is monotone: an admitted signature stays admitted, and a full cap stays
// full. That is what keeps paired call sites consistent — the in-flight gauge's Add at
// relay start and Sub at relay end normalize the same api name to the same string, so
// the gauge cannot leak a stuck non-zero series.
func (r *batchSignatureRegistry) admit(spec, signature string) bool {
	if r == nil {
		return false
	}

	r.lock.RLock()
	if signatures, ok := r.admitted[spec]; ok {
		if _, alreadyAdmitted := signatures[signature]; alreadyAdmitted {
			r.lock.RUnlock()
			return true
		}
	}
	r.lock.RUnlock()

	r.lock.Lock()
	defer r.lock.Unlock()

	signatures, ok := r.admitted[spec]
	if !ok {
		signatures = make(map[string]struct{}, r.cap)
		r.admitted[spec] = signatures
	}
	if _, alreadyAdmitted := signatures[signature]; alreadyAdmitted {
		return true
	}
	if len(signatures) >= r.cap {
		return false
	}
	signatures[signature] = struct{}{}
	return true
}

// normalizeMethodLabel collapses a raw batch api name into a bounded signature and
// returns single-method names untouched.
//
// It is idempotent, because a collapsed value carries no batchMethodSeparator. That
// matters: the public recorders delegate to one another (RecordDirectRelayEnd →
// RecordRelaySuccess → AddEndpointRelayServiced), and each normalizes at its own
// boundary so that calling any of them directly is still safe.
func (m *SmartRouterMetricsManager) normalizeMethodLabel(spec, method string) string {
	if m == nil {
		return method
	}
	if !strings.Contains(method, batchMethodSeparator) {
		// Single-method names have their own unbounded case: the synthetic
		// "Default-<raw request>" apis minted for spec misses (default_method_label.go).
		return m.normalizeDefaultMethodLabel(spec, method)
	}

	signature, _, tooWide := buildBatchSignature(method)
	if tooWide {
		m.recordBatchSignatureOverflow(spec, batchOverflowReasonWide)
		return BatchMethodOther
	}
	if !m.batchSignatures.admit(spec, signature) {
		m.recordBatchSignatureOverflow(spec, batchOverflowReasonCap)
		return BatchMethodOther
	}
	return signature
}

func (m *SmartRouterMetricsManager) recordBatchSignatureOverflow(spec, reason string) {
	if m == nil || m.batchSignatureOverflow == nil {
		return
	}
	m.batchSignatureOverflow.WithLabelValues(spec, reason).Inc()
}

// observeBatchSize records how many sub-requests a batch carried — the magnitude that
// the collapsed `method` label deliberately no longer encodes.
//
// This must be called from exactly ONE place (RecordDirectRelayEnd, once per completed
// relay). The other recorders normalize but must not observe, or a single request would
// be counted once per recorder it passes through.
//
// Single-element batches carry no separator in their api name, so they are
// indistinguishable from single requests here and are not observed.
func (m *SmartRouterMetricsManager) observeBatchSize(spec, apiInterface, method string) {
	if m == nil || m.batchSize == nil || !strings.Contains(method, batchMethodSeparator) {
		return
	}

	_, size, _ := buildBatchSignature(method)
	if size == 0 {
		return
	}
	m.batchSize.WithLabelValues(spec, apiInterface).Observe(float64(size))
}
