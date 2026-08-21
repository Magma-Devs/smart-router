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

	// defaultMethodIDPlaceholder stands in for a path segment that carries a concrete
	// value. It deliberately does not name the parameter: unlike a matched api, an
	// unmatched path has no spec template to take a name from, and inventing one would
	// make the label look like a template it is not.
	defaultMethodIDPlaceholder = "{}"

	// minOpaqueIDLen is the length at or above which an alphanumeric segment carrying a
	// digit is read as an identifier rather than a path element. Measured over every
	// REST api segment in the specs: the longest real route segment carrying a digit is
	// 39 chars (TRX's gettriggerinputforshieldedtrc20contract) and the shortest real
	// identifier class starts at 43 (base58 of a 32-byte hash is 43-44 chars; bech32
	// addresses 44-45, Tezos block hashes 51). 43 is the top of that gap: it still
	// catches every identifier class while maximizing route-segment headroom, which is
	// the direction to err in — an under-collapsed value costs a budget entry the cap
	// absorbs, an over-collapsed route segment merges two real endpoints into one
	// series and cannot be undone.
	minOpaqueIDLen = 43
)

// defaultMethodShape collapses concrete identifiers in an unmatched REST path to
// defaultMethodIDPlaceholder, so /blocks/1/header and /blocks/12/header become the one
// shape /blocks/{}/header. The per-spec budget is then spent on path SHAPES rather than
// on one entry per block number, which is what keeps a flood from reaching the cap at
// all — the cap stays the backstop for genuinely pathological specs.
//
// Only paths are reshaped. A JSON-RPC method name is already a stable identifier and
// several carry digits that are part of the name (web3_clientVersion,
// eth_getBlockByNumber), so anything not starting with "/" is returned untouched.
//
// The result is idempotent: defaultMethodIDPlaceholder is not itself a concrete
// identifier, so reshaping an already-reshaped path is a no-op. That matters because
// the public recorders delegate to one another and normalize at each boundary.
func defaultMethodShape(method string) string {
	raw := strings.TrimPrefix(method, defaultMethodPrefix)
	if !strings.HasPrefix(raw, "/") {
		return method
	}

	segments := strings.Split(raw, "/")
	reshaped := false
	for i, segment := range segments {
		if isConcreteIDSegment(segment) {
			segments[i] = defaultMethodIDPlaceholder
			reshaped = true
		}
	}
	if !reshaped {
		return method
	}
	return defaultMethodPrefix + strings.Join(segments, "/")
}

// isConcreteIDSegment reports whether one path segment is a value rather than part of
// the route: a block number, a hex address, or an opaque hash. A segment that mixes
// letters without carrying an identifier's length is left alone, which is what keeps
// route elements such as "v1", "head", "latest" and "ledger_info" intact.
func isConcreteIDSegment(segment string) bool {
	if segment == "" || segment == defaultMethodIDPlaceholder {
		return false
	}
	if isAllDigits(segment) {
		return true
	}
	if len(segment) > 2 && (strings.HasPrefix(segment, "0x") || strings.HasPrefix(segment, "0X")) && isHex(segment[2:]) {
		return true
	}
	return len(segment) >= minOpaqueIDLen && hasDigit(segment) && isIDCharset(segment)
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

func hasDigit(s string) bool {
	for _, r := range s {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}

// isIDCharset reports whether every rune could belong to an address or hash: letters,
// digits, and the few separators real identifiers use (bech32 "lava@1...", prefixed
// ids). A segment containing anything else is not treated as opaque.
func isIDCharset(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r == '@' || r == '-' || r == '_' || r == '.' || r == ':':
		default:
			return false
		}
	}
	return true
}

// normalizeDefaultMethodLabel bounds the method label for unmatched ("Default-*")
// APIs in two steps: concrete identifiers in a path are collapsed to a shape
// (defaultMethodShape), then the first maxDefaultMethodsPerSpec distinct shapes per
// spec pass through and everything after collapses to DefaultMethodOther.
//
// Admission is monotone (see batchSignatureRegistry), so the same raw name maps to
// the same label value for the lifetime of the process — paired call sites such as
// the in-flight gauge's Add/Sub stay consistent. DefaultMethodOther itself passes
// through unconditionally, which is what makes the collapse idempotent.
func (m *SmartRouterMetricsManager) normalizeDefaultMethodLabel(spec, method string) string {
	if !strings.HasPrefix(method, defaultMethodPrefix) || method == DefaultMethodOther {
		return method
	}
	// Reshape before admitting so the budget counts shapes, not instances.
	method = defaultMethodShape(method)
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
