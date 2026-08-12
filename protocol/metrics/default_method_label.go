package metrics

import "strings"

const (
	// defaultMethodPrefix mirrors chainlib's DefaultApiName — the prefix the chain
	// parser stamps on the synthetic Api it builds for a request that matched no
	// spec API. It is redeclared here rather than imported because chainlib already
	// imports this package; importing back would be a cycle (same situation as
	// batchMethodSeparator in batch_method_label.go).
	defaultMethodPrefix = "Default-"

	// DefaultMethodOther is the overflow bucket for unmatched-API method names past
	// the per-spec cap. Unmatched REST paths carry concrete values (block numbers,
	// addresses) where a matched API would carry a {placeholder}, so left alone the
	// label mints a new series per URL — 46k distinct method values observed on one
	// deployment from a single client polling /blocks/<n>/header/ with a trailing
	// slash the spec template did not match.
	DefaultMethodOther = defaultMethodPrefix + "other"

	// maxDefaultMethodsPerSpec caps how many distinct Default-* names one spec may
	// register. A genuinely-missing spec API produces ONE stable name — the useful
	// spec-gap signal this cap preserves — while a concrete-ID flood blows through
	// the budget immediately and every later name collapses to DefaultMethodOther.
	maxDefaultMethodsPerSpec = 32
)

// normalizeDefaultMethodLabel bounds the method label for unmatched ("Default-*")
// APIs: the first maxDefaultMethodsPerSpec distinct names per spec pass through,
// everything after collapses to DefaultMethodOther.
//
// Admission is monotone (see batchSignatureRegistry), so the same raw name maps to
// the same label value for the lifetime of the process — paired call sites such as
// the in-flight gauge's Add/Sub stay consistent. DefaultMethodOther itself passes
// through unconditionally, which is what makes the collapse idempotent.
func (m *SmartRouterMetricsManager) normalizeDefaultMethodLabel(spec, method string) string {
	if !strings.HasPrefix(method, defaultMethodPrefix) || method == DefaultMethodOther {
		return method
	}
	if !m.defaultMethods.admit(spec, method) {
		m.recordDefaultMethodOverflow(spec)
		return DefaultMethodOther
	}
	return method
}

func (m *SmartRouterMetricsManager) recordDefaultMethodOverflow(spec string) {
	if m == nil || m.defaultMethodOverflow == nil {
		return
	}
	m.defaultMethodOverflow.WithLabelValues(spec).Inc()
}
