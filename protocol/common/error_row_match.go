package common

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// Error rows (PRD §2.2; MAG-5597) match the error record a reply yields (§3.1). Every match key a row
// has must hold: status (an HTTP status as a number, a gRPC status by the rulebook's name), code (by
// JSON type and value), contains (case-insensitive) or regex (RE2) on the record's text or, with
// field: detail, on its detail, and on GraphQL the error's path. The first row that matches wins, in
// the order the policy lists them (inheritance order, MAG-5606).

// grpcStatusNames are the rulebook's names for the gRPC status codes a row's status may name.
var grpcStatusNames = map[string]int{
	"CANCELLED": 1, "UNKNOWN": 2, "INVALID_ARGUMENT": 3, "DEADLINE_EXCEEDED": 4, "NOT_FOUND": 5,
	"ALREADY_EXISTS": 6, "PERMISSION_DENIED": 7, "RESOURCE_EXHAUSTED": 8, "FAILED_PRECONDITION": 9,
	"ABORTED": 10, "OUT_OF_RANGE": 11, "UNIMPLEMENTED": 12, "INTERNAL": 13, "UNAVAILABLE": 14,
	"DATA_LOSS": 15, "UNAUTHENTICATED": 16,
}

// GRPCStatusCodeNamed is the gRPC status code the rulebook's name stands for ("NOT_FOUND" is 5).
func GRPCStatusCodeNamed(name string) (int, bool) {
	code, ok := grpcStatusNames[name]
	return code, ok
}

// MatchErrorRow is the first of rows that matches record, a reply to method; isWrite says whether the
// request is a stateful api, which a row's scope reads. Answer rows and disabled rows are skipped: an
// answer row reads an answer (MatchAnswerRow), not a record.
func MatchErrorRow(rows []*spectypes.ErrorRow, record ErrorRecord, method string, isWrite bool) (*spectypes.ErrorRow, bool) {
	for _, row := range rows {
		if row == nil || row.Disabled || row.Answer || !row.AppliesToInterface(record.Interface) || !row.AppliesToMethod(method) {
			continue
		}
		switch row.EffectiveScope() {
		case spectypes.ErrorRowScopeWrite:
			if !isWrite {
				continue
			}
		case spectypes.ErrorRowScopeRead:
			if isWrite {
				continue
			}
		}
		if errorRowMatches(row, record) {
			return row, true
		}
	}
	return nil, false
}

// errorRowMatches reports whether every match key of row holds on record. A row with no match key
// matches nothing: the validator refuses it, and here it is inert rather than catching every reply.
func errorRowMatches(row *spectypes.ErrorRow, record ErrorRecord) bool {
	keys := 0
	if len(row.Status) > 0 {
		keys++
		if !statusMatches(row.Status, record) {
			return false
		}
	}
	if row.HasCode() {
		keys++
		if !JSONValueEquals(record.Code, row.Code) {
			return false
		}
	}
	if row.Contains != "" || row.Regex != "" {
		keys++
		text := record.Text
		if row.EffectiveField() == spectypes.ErrorRowFieldDetail {
			text = record.DetailText()
			if text == "" {
				return false // a null or absent detail matches nothing
			}
		}
		if row.Contains != "" && !strings.Contains(strings.ToLower(text), strings.ToLower(row.Contains)) {
			return false
		}
		if row.Regex != "" {
			re, err := compiledRowRegex(row.Regex)
			if err != nil || !re.MatchString(text) {
				return false
			}
		}
	}
	if len(row.Path) > 0 {
		keys++
		if !pathMatches(row.Path, record) {
			return false
		}
	}
	return keys > 0
}

// statusMatches reports whether the record's status is the row's: an HTTP status by number; a gRPC
// status by the rulebook's name, read against the gRPC code the reply carried (GRPCCode, which a
// gateway's HTTP status does not replace).
func statusMatches(raw json.RawMessage, record ErrorRecord) bool {
	var want any
	if err := json.Unmarshal(raw, &want); err != nil {
		return false
	}
	switch status := want.(type) {
	case float64:
		if record.Interface == spectypes.APIInterfaceGrpc {
			// On grpc a number is an HTTP status: only a gateway's (D12), never the gRPC code.
			return record.Status != record.GRPCCode && record.Status == int(status)
		}
		return record.Status == int(status)
	case string:
		code, ok := GRPCStatusCodeNamed(status)
		return ok && record.Interface == spectypes.APIInterfaceGrpc && record.GRPCCode == code
	default:
		return false
	}
}

// pathMatches reports whether a GraphQL error's path is what the row asks: false, no path; true, any
// path; a name, a path that starts there (the reader has already put the field's name in an alias's
// place).
func pathMatches(raw json.RawMessage, record ErrorRecord) bool {
	if record.Interface != spectypes.APIInterfaceGraphQL {
		return false
	}
	var want any
	if err := json.Unmarshal(raw, &want); err != nil {
		return false
	}
	switch p := want.(type) {
	case bool:
		return (len(record.Path) > 0) == p
	case string:
		return len(record.Path) > 0 && record.Path[0] == p
	default:
		return false
	}
}

var (
	rowRegexMu    sync.Mutex
	rowRegexCache = map[string]*regexp.Regexp{}
)

// compiledRowRegex compiles a row's regex once; a row that does not compile matches nothing (the
// validator refuses it before it ships).
func compiledRowRegex(pattern string) (*regexp.Regexp, error) {
	rowRegexMu.Lock()
	defer rowRegexMu.Unlock()
	if re, ok := rowRegexCache[pattern]; ok {
		return re, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	rowRegexCache[pattern] = re
	return re, nil
}
