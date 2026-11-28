// Package specvalidate is the spec validator lava-specs CI and the router share (PRD §4,
// MAG-5604): it checks a spec's `errors` section against the contract, reading every list it
// enforces from the router (common, keeper) rather than keeping its own.
//
// This is the per-section part of §4: the schema, §4.1's vocabulary, names and row and shape
// rules, §4.2's verdicts, §4.3's matches, and §4.5's table. What reads a chain's effective
// (inherited) rows waits on the spec reader's inheritance (MAG-5606): one name with two classes, a
// migrated verdict's flags, the write-safety replay and a parent's matcher reaching a descendant's
// stateful api. So do §4.4's source evidence, a gRPC shape's bundled method, and boot validation.
package specvalidate

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The reasons a section is dropped for (PRD §5.6, smartrouter_spec_errors_section_dropped_total).
const (
	ReasonSchemaUnknown = "schema_unknown"
	ReasonRuleViolation = "rule_violation"
)

// SchemaVersion is the `errors` format this router reads.
const SchemaVersion = 1

// Finding is one thing a spec's `errors` section breaks.
type Finding struct {
	Spec string `json:"spec"`
	// Item is the row's or shape's id, "" for the section itself.
	Item string `json:"item,omitempty"`
	// Rule names the PRD rule, e.g. "4.1/class".
	Rule string `json:"rule"`
	// Reason is the drop reason a boot check counts: schema_unknown or rule_violation.
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

func (f Finding) String() string {
	if f.Item == "" {
		return fmt.Sprintf("%s: [%s] %s", f.Spec, f.Rule, f.Detail)
	}
	return fmt.Sprintf("%s %s: [%s] %s", f.Spec, f.Item, f.Rule, f.Detail)
}

var apiInterfaces = map[string]bool{
	spectypes.APIInterfaceJsonRPC:       true,
	spectypes.APIInterfaceTendermintRPC: true,
	spectypes.APIInterfaceRest:          true,
	spectypes.APIInterfaceGrpc:          true,
	spectypes.APIInterfaceGraphQL:       true,
}

var (
	classes = map[string][]string{
		spectypes.ErrorClassC2: {spectypes.ErrorReasonInvalid, spectypes.ErrorReasonOverLimit},
		spectypes.ErrorClassC3: {
			spectypes.ErrorReasonNotFound, spectypes.ErrorReasonPruned, spectypes.ErrorReasonBehind,
			spectypes.ErrorReasonLimit, spectypes.ErrorReasonVersion, spectypes.ErrorReasonMethodOff,
		},
		spectypes.ErrorClassC4: nil,
		spectypes.ErrorClassC5: nil,
	}
	writeVerdicts = []string{spectypes.ErrorWriteTookIt, spectypes.ErrorWriteRefusedIt, spectypes.ErrorWriteMayHaveRun}
	scopes        = []string{spectypes.ErrorRowScopeRead, spectypes.ErrorRowScopeWrite, spectypes.ErrorRowScopeBoth}
	fields        = []string{spectypes.ErrorRowFieldText, spectypes.ErrorRowFieldDetail}
	rowIDPattern  = regexp.MustCompile(`^[a-z][a-z0-9]*\.[a-z0-9]+(_[a-z0-9]+)*$`)
)

// Section checks spec's own `errors` section. served is every interface the spec serves, its
// imports included (ServedInterfaces); nil when the caller can't tell, which skips the checks that
// need it.
func Section(spec spectypes.Spec, served map[string]bool) []Finding {
	section := spec.Errors
	if section == nil {
		return nil
	}
	v := &validator{spec: spec.Index, served: served, names: ownNamePattern()}
	if section.SchemaVersion != SchemaVersion {
		v.add("", "4.1/schema_version", ReasonSchemaUnknown, "schema_version %d: this router reads %d", section.SchemaVersion, SchemaVersion)
		return v.findings
	}
	for _, apiInterface := range section.WriteRows {
		v.checkInterface("", "2.1/write_rows", apiInterface)
	}
	shapeIDs := map[string]bool{}
	for _, shape := range section.Shapes {
		if shape == nil {
			continue
		}
		if shapeIDs[shape.ID] {
			v.add(shape.ID, "2.4/id", ReasonRuleViolation, "a second shape with this id")
		}
		shapeIDs[shape.ID] = true
		v.shape(shape)
	}
	rowIDs := map[string]bool{}
	for _, row := range section.Rows {
		if row == nil {
			continue
		}
		if rowIDs[row.ID] {
			v.add(row.ID, "2.2/id", ReasonRuleViolation, "a second row with this id in the same spec")
		}
		rowIDs[row.ID] = true
		v.row(row)
	}
	return v.findings
}

// ServedInterfaces is every interface the spec index serves: its own collections' and, through its
// imports, theirs. ok is false when an import is missing from specs.
func ServedInterfaces(specs map[string]spectypes.Spec, index string) (served map[string]bool, ok bool) {
	served = map[string]bool{}
	ok = true
	seen := map[string]bool{}
	var walk func(string)
	walk = func(index string) {
		if seen[index] {
			return
		}
		seen[index] = true
		spec, found := specs[index]
		if !found {
			ok = false
			return
		}
		for _, collection := range spec.ApiCollections {
			if collection != nil && collection.CollectionData.ApiInterface != "" {
				served[collection.CollectionData.ApiInterface] = true
			}
		}
		for _, imported := range spec.Imports {
			walk(imported)
		}
	}
	walk(index)
	return served, ok
}

type validator struct {
	spec     string
	served   map[string]bool
	names    *regexp.Regexp
	findings []Finding
}

func (v *validator) add(item, rule, reason, format string, args ...any) {
	v.findings = append(v.findings, Finding{Spec: v.spec, Item: item, Rule: rule, Reason: reason, Detail: fmt.Sprintf(format, args...)})
}

func (v *validator) checkInterface(item, rule, apiInterface string) {
	if !apiInterfaces[apiInterface] {
		v.add(item, rule, ReasonRuleViolation, "%q is not an interface (jsonrpc, tendermintrpc, rest, grpc, graphql)", apiInterface)
		return
	}
	if v.served != nil && !v.served[apiInterface] {
		v.add(item, rule, ReasonRuleViolation, "%s is not an interface this spec or its imports serve", apiInterface)
	}
}

func (v *validator) shape(shape *spectypes.ErrorShape) {
	if shape.Disabled {
		return
	}
	if shape.ID == "" {
		v.add("", "2.4/id", ReasonRuleViolation, "a shape without an id")
	}
	v.checkInterface(shape.ID, "4.1/shape_interface", shape.Interface)
	pointers := []string{shape.Code, shape.Space, shape.Message}
	if shape.When != nil {
		pointers = append(pointers, shape.When.At)
	}
	for _, pointer := range pointers {
		if pointer != "" && !common.IsJSONPointer(pointer) {
			v.add(shape.ID, "4.1/shape_pointer", ReasonRuleViolation, "%q is not a JSON Pointer (RFC 6901)", pointer)
		}
	}
}

func (v *validator) row(row *spectypes.ErrorRow) {
	id := row.ID
	if !rowIDPattern.MatchString(id) {
		v.add(id, "2.2/id", ReasonRuleViolation, "an id is <family>.<snake_case>")
	}
	if row.Disabled {
		if len(row.Interface) > 0 || len(row.Methods) > 0 || row.Error != "" || row.Class != "" || row.HasCode() || row.Contains != "" || row.Regex != "" {
			v.add(id, "2.2/disabled", ReasonRuleViolation, "a disabled row carries only id, disabled and note")
		}
		return
	}
	if len(row.Interface) == 0 {
		v.add(id, "2.2/interface", ReasonRuleViolation, "a row names its interface")
	}
	for _, apiInterface := range row.Interface {
		v.checkInterface(id, "2.2/interface", apiInterface)
	}
	if row.Scope != "" && !oneOf(row.Scope, scopes) {
		v.add(id, "2.2/scope", ReasonRuleViolation, "scope %q: read, write or both", row.Scope)
	}
	v.matchers(row)
	v.verdict(row)
	if row.Answer {
		v.answerRow(row)
	} else {
		v.errorRow(row)
	}
}

// matchers checks a row's match keys: their own rules (§2.2) and the ones a row may not use (§4.3).
func (v *validator) matchers(row *spectypes.ErrorRow) {
	id := row.ID
	if row.Contains != "" && row.Regex != "" {
		v.add(id, "2.2/regex", ReasonRuleViolation, "contains and regex together")
	}
	var re *regexp.Regexp
	if row.Regex != "" {
		compiled, err := regexp.Compile(row.Regex)
		if err != nil {
			v.add(id, "2.2/regex", ReasonRuleViolation, "the regex does not compile: %v", err)
		} else {
			re = compiled
		}
	}
	if row.Field != "" {
		if !oneOf(row.Field, fields) {
			v.add(id, "2.2/field", ReasonRuleViolation, "field %q: text or detail", row.Field)
		}
		if row.Contains == "" && row.Regex == "" {
			v.add(id, "2.2/field", ReasonRuleViolation, "field without contains or regex")
		}
	}
	if len(row.Path) > 0 && !(len(row.Interface) == 1 && row.Interface[0] == spectypes.APIInterfaceGraphQL) {
		v.add(id, "4.1/path", ReasonRuleViolation, "path is GraphQL's only")
	}
	if len(row.Status) > 0 {
		v.status(row)
	}
	if row.HasCode() && is429(row.Code) {
		v.add(id, "4.3/code", ReasonRuleViolation, "code 429: a 429 code inside a 2xx body is unknown (Part IV decision 1)")
	}
	matches := func(text string) bool {
		lower := strings.ToLower(text)
		if row.Contains != "" {
			c := strings.ToLower(row.Contains)
			return strings.Contains(lower, c) || strings.Contains(c, lower)
		}
		return re != nil && re.MatchString(text)
	}
	if row.Contains == "" && re == nil {
		return
	}
	for _, status := range common.StatusDecidedHTTPStatuses {
		if prefix := fmt.Sprintf("HTTP %d: ", status); matches(prefix) {
			v.add(id, "4.3/text", ReasonRuleViolation, "matches the router's %q prefix: the status %d decides", prefix, status)
		}
	}
	if matches(common.GRPCGatewayStatusText) {
		v.add(id, "4.3/text", ReasonRuleViolation, "matches grpc-go's %q: a gateway's status, not the node's answer", common.GRPCGatewayStatusText)
	}
	if appliesToAny(row, spectypes.APIInterfaceRest, spectypes.APIInterfaceGraphQL) {
		for _, phrase := range common.RouteRefusalPhrases() {
			if matches(phrase) {
				v.add(id, "4.3/text", ReasonRuleViolation, "matches the route-refusal phrase %q the router's REST rows read (until MAG-5253)", phrase)
			}
		}
	}
}

// status checks a row's status against each interface it applies to (§2.2, §4.3).
func (v *validator) status(row *spectypes.ErrorRow) {
	id := row.ID
	var number int
	var name string
	if err := json.Unmarshal(row.Status, &number); err != nil {
		if err := json.Unmarshal(row.Status, &name); err != nil {
			v.add(id, "2.2/status", ReasonRuleViolation, "status %s: an HTTP status number or a gRPC status name", string(row.Status))
			return
		}
	}
	for _, apiInterface := range row.Interface {
		if apiInterface == spectypes.APIInterfaceGrpc {
			if name == "" {
				v.add(id, "4.3/status", ReasonRuleViolation, "status %s is not a gRPC status: a gRPC row names one (\"NOT_FOUND\")", string(row.Status))
				continue
			}
			if _, ok := common.GRPCStatusCodeNamed(name); !ok {
				v.add(id, "4.3/status", ReasonRuleViolation, "%q is not a gRPC status", name)
				continue
			}
			if oneOf(name, common.StatusDecidedGRPCStatuses) {
				v.add(id, "4.3/status", ReasonRuleViolation, "the status %s decides: never a row's", name)
			}
			if name == "RESOURCE_EXHAUSTED" && !(row.Class == spectypes.ErrorClassC4 ||
				(row.Class == spectypes.ErrorClassC2 && row.EffectiveReason() == spectypes.ErrorReasonOverLimit)) {
				v.add(id, "4.3/status", ReasonRuleViolation, "RESOURCE_EXHAUSTED only on a C2 over_limit or a C4 row")
			}
			continue
		}
		if name != "" || number < 100 || number > 599 {
			v.add(id, "4.3/status", ReasonRuleViolation, "status %s is not an HTTP status, which %s replies carry", string(row.Status), apiInterface)
			continue
		}
		for _, decided := range common.StatusDecidedHTTPStatuses {
			if number == decided {
				v.add(id, "4.3/status", ReasonRuleViolation, "the status %d decides: never a row's", number)
			}
		}
	}
}

// verdict checks the row's error, class, reason and write (§4.1, §4.2).
func (v *validator) verdict(row *spectypes.ErrorRow) {
	id := row.ID
	if row.Class != "" {
		reasons, known := classes[row.Class]
		switch {
		case !known:
			v.add(id, "4.1/class", ReasonRuleViolation, "class %q: a row gives C2, C3, C4 or C5", row.Class)
		case row.Reason != "" && !oneOf(row.Reason, reasons):
			v.add(id, "4.1/reason", ReasonRuleViolation, "reason %q: class %s takes %s", row.Reason, row.Class, describe(reasons))
		case row.Class == spectypes.ErrorClassC3 && row.Reason == "":
			v.add(id, "4.1/reason", ReasonRuleViolation, "a C3 row gives its reason")
		}
	} else if row.Reason != "" {
		v.add(id, "4.1/reason", ReasonRuleViolation, "a reason without a class")
	}
	if row.Write != "" {
		if !oneOf(row.Write, writeVerdicts) {
			v.add(id, "4.1/write", ReasonRuleViolation, "write %q: took_it, refused_it or may_have_run", row.Write)
		}
		if row.Scope == spectypes.ErrorRowScopeRead {
			v.add(id, "4.1/write", ReasonRuleViolation, "write on a read row")
		}
	}
	if row.Error == "" {
		return
	}
	routerError, registered := common.RouterErrorByName(row.Error)
	if registered {
		if reason := forbiddenVerdict(routerError, row); reason != "" {
			v.add(id, "4.2/error", ReasonRuleViolation, "%s: %s", row.Error, reason)
		}
		return
	}
	// The chain's own verdict: named for its family, with a class (and a C3's reason).
	if !v.names.MatchString(row.Error) {
		v.add(id, "4.1/error", ReasonRuleViolation, "%s: a chain's own verdict is CHAIN_<FAMILY>_* or NODE_<FAMILY>_*", row.Error)
	}
	if row.Class == "" {
		v.add(id, "4.1/class", ReasonRuleViolation, "%s is the chain's own verdict: the row gives its class", row.Error)
	}
}

// forbiddenVerdict is why no row may name routerError (§4.2), "" when one may.
func forbiddenVerdict(routerError *common.RouterError, row *spectypes.ErrorRow) string {
	switch {
	case routerError == common.RouterErrorUnknown:
		return "the router's unknown default"
	case strings.HasPrefix(routerError.Name, "PROTOCOL_"):
		return "a protocol verdict"
	case routerError.SubCategory == common.SubCategoryRateLimit:
		return "a rate-limit verdict: a chain's quota row names its own C4 verdict"
	case routerError == common.RouterErrorChainTxAlreadyKnown && row.Write != spectypes.ErrorWriteTookIt:
		return "only a duplicate row (write: took_it) names it"
	}
	for _, decided := range common.StatusDecidedVerdicts {
		if routerError == decided {
			return "C6: the status decides it"
		}
	}
	return ""
}

// answerRow checks a row that reads a 2xx answer (§2.2, §4.1).
func (v *validator) answerRow(row *spectypes.ErrorRow) {
	id := row.ID
	if len(row.Methods) == 0 {
		v.add(id, "4.1/answer", ReasonRuleViolation, "an answer row names its methods")
	}
	if row.Contains != "" || row.Regex != "" || len(row.Status) > 0 || len(row.Path) > 0 {
		v.add(id, "4.1/answer", ReasonRuleViolation, "an answer row matches by at with code or present, nothing else")
	}
	if row.At == "" && (row.HasCode() || row.Present != nil) {
		v.add(id, "4.1/answer", ReasonRuleViolation, "code or present on an answer row needs at")
	}
	if row.At != "" {
		if !common.IsJSONPointer(row.At) {
			v.add(id, "4.1/answer", ReasonRuleViolation, "at %q is not a JSON Pointer (RFC 6901)", row.At)
		}
		if !row.HasCode() && row.Present == nil {
			v.add(id, "4.1/answer", ReasonRuleViolation, "at with code or present")
		}
	}
	success := row.Write == spectypes.ErrorWriteTookIt && row.Error == "" && row.Class == "" && row.Reason == ""
	notFound := row.Error != "" && row.Class == spectypes.ErrorClassC3 && row.Reason == spectypes.ErrorReasonNotFound && row.Write == ""
	if !success && !notFound {
		v.add(id, "4.1/answer", ReasonRuleViolation, "an answer row gives a success (write: took_it alone) or a not-found (error, C3, not_found)")
	}
}

// errorRow checks a row that reads an error record (§2.2, §4.1).
func (v *validator) errorRow(row *spectypes.ErrorRow) {
	id := row.ID
	if row.At != "" || row.Present != nil {
		v.add(id, "4.1/at", ReasonRuleViolation, "at and present are an answer row's")
	}
	if row.Error == "" {
		v.add(id, "2.2/error", ReasonRuleViolation, "an error row names its verdict")
	}
	if row.HasCode() || row.Contains != "" || row.Regex != "" {
		return
	}
	statusAlone := len(row.Status) > 0 && (onlyInterface(row, spectypes.APIInterfaceGrpc) || string(row.Status) == "409")
	if !statusAlone {
		v.add(id, "2.2/match", ReasonRuleViolation, "an error row matches code, contains or regex (status alone only on a gRPC-only row or a bare HTTP 409)")
	}
}

// ownNamePattern is CHAIN_<FAMILY>_* / NODE_<FAMILY>_* over the registry's families.
func ownNamePattern() *regexp.Regexp {
	families := common.ChainFamilies()
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, regexp.QuoteMeta(strings.ToUpper(family.String())))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	return regexp.MustCompile(`^(CHAIN|NODE)_(` + strings.Join(names, "|") + `)_[A-Z0-9]+(_[A-Z0-9]+)*$`)
}

func is429(raw json.RawMessage) bool {
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String() == "429"
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return strings.TrimSpace(text) == strconv.Itoa(429)
	}
	return false
}

func appliesToAny(row *spectypes.ErrorRow, apiInterfaces ...string) bool {
	for _, apiInterface := range row.Interface {
		if oneOf(apiInterface, apiInterfaces) {
			return true
		}
	}
	return false
}

func onlyInterface(row *spectypes.ErrorRow, apiInterface string) bool {
	return len(row.Interface) == 1 && row.Interface[0] == apiInterface
}

func oneOf(value string, set []string) bool {
	for _, member := range set {
		if value == member {
			return true
		}
	}
	return false
}

func describe(reasons []string) string {
	if len(reasons) == 0 {
		return "none"
	}
	return strings.Join(reasons, ", ")
}
