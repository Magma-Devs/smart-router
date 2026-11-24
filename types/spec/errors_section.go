package spec

import (
	"encoding/json"
	"fmt"
)

// The `errors` section of a spec (PRD PER-CHAIN-ERROR-CLASSIFICATION §2; MAG-5595): where a chain's
// errors sit in its replies (shapes), how they are classified (rows), which interfaces' write rows
// have shipped, and which REST methods answer in plain text. These are the wire types, as a spec
// file writes them: decoding keeps every value a row may match by JSON type (a code of -32005,
// "sdk/32", true or null) as raw JSON. Inheritance (DoExpandSpec, MAG-5606) and validation
// (cmd/specvalidate, MAG-5604) read them; the router's readers act on them through
// protocol/common's error record and shapes engine. A spec without the section keeps today's
// behaviour; an older router drops the unknown key.

// ErrorsSchemaVersion is the schema_version this router reads. A section with another version is
// dropped whole (PRD §2.1).
const ErrorsSchemaVersion = 1

// Row scopes (PRD §2.2): a write row runs only for stateful apis, a read row only for the rest.
const (
	ErrorRowScopeRead  = "read"
	ErrorRowScopeWrite = "write"
	ErrorRowScopeBoth  = "both"
)

// What a row's contains or regex reads (PRD §2.2).
const (
	ErrorRowFieldText   = "text"
	ErrorRowFieldDetail = "detail"
)

// The rulebook's classes a row may give a chain's own verdict (PRD §2.3).
const (
	ErrorClassC2 = "C2"
	ErrorClassC3 = "C3"
	ErrorClassC4 = "C4"
	ErrorClassC5 = "C5"
)

// A row's reasons (PRD §2.3): invalid and over_limit on C2; the rest on C3.
const (
	ErrorReasonInvalid   = "invalid"
	ErrorReasonOverLimit = "over_limit"
	ErrorReasonNotFound  = "not_found"
	ErrorReasonPruned    = "pruned"
	ErrorReasonBehind    = "behind"
	ErrorReasonLimit     = "limit"
	ErrorReasonVersion   = "version"
	ErrorReasonMethodOff = "method_off"
)

// A row's write verdict for a write copy (PRD §5.2, rulebook R-L).
const (
	ErrorWriteTookIt     = "took_it"
	ErrorWriteRefusedIt  = "refused_it"
	ErrorWriteMayHaveRun = "may_have_run"
)

// ErrorsSection is the `errors` key of a spec (PRD §2.1).
type ErrorsSection struct {
	SchemaVersion int           `json:"schema_version"`
	Shapes        []*ErrorShape `json:"shapes,omitempty"`
	Rows          []*ErrorRow   `json:"rows,omitempty"`
	// WriteRows names the interfaces whose write rows have shipped; it acts only at stage `all`
	// and is never inherited (PRD §2.1).
	WriteRows []string `json:"write_rows,omitempty"`
	// PlainText names the REST methods whose 2xx plain-text body is an answer (Part V R-D6).
	PlainText []string `json:"plain_text,omitempty"`
	// Inherit false inherits no rows; the parents' shapes and plain_text still apply. Nil is true.
	Inherit *bool `json:"inherit,omitempty"`
}

// Inherits reports whether the section takes its parents' rows: the default, and a nil section's.
func (s *ErrorsSection) Inherits() bool {
	return s == nil || s.Inherit == nil || *s.Inherit
}

// ErrorRow is one row of the section (PRD §2.2): an error row matches the error record a reply
// yields; an answer row (Answer true) matches the reply's answer through At, Code and Present.
type ErrorRow struct {
	ID        string        `json:"id"`
	Interface InterfaceList `json:"interface,omitempty"`
	Methods   []string      `json:"methods,omitempty"`
	Scope     string        `json:"scope,omitempty"`
	Answer    bool          `json:"answer,omitempty"`
	// Status is the reply's status: an HTTP status as a number, a gRPC status as the rulebook
	// writes it ("NOT_FOUND"). Raw, so the two are told apart by JSON type.
	Status json.RawMessage `json:"status,omitempty"`
	// Code is the record's code, or with At the value there: a number, string, boolean or null,
	// matched by JSON type and value. Raw, so an explicit null (a not-found row) is kept and told
	// apart from an absent key: len(Code) > 0 when the row carries one.
	Code     json.RawMessage `json:"code,omitempty"`
	Contains string          `json:"contains,omitempty"`
	Regex    string          `json:"regex,omitempty"`
	Field    string          `json:"field,omitempty"`
	// Path, GraphQL only: false (no path), true (any), or a root field name. Raw for the same
	// reason as Code.
	Path    json.RawMessage `json:"path,omitempty"`
	At      string          `json:"at,omitempty"`
	Present *bool           `json:"present,omitempty"`
	Error   string          `json:"error,omitempty"`
	Class   string          `json:"class,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Write   string          `json:"write,omitempty"`
	Source  string          `json:"source,omitempty"`
	// Disabled true removes the inherited row with this ID for this spec and its children.
	Disabled bool   `json:"disabled,omitempty"`
	Note     string `json:"note,omitempty"`
}

// HasCode reports whether the row carries a code to match, an explicit null included.
func (r *ErrorRow) HasCode() bool { return len(r.Code) > 0 }

// EffectiveScope is the row's scope with the default, both.
func (r *ErrorRow) EffectiveScope() string {
	if r.Scope == "" {
		return ErrorRowScopeBoth
	}
	return r.Scope
}

// EffectiveField is what contains or regex read, text by default.
func (r *ErrorRow) EffectiveField() string {
	if r.Field == "" {
		return ErrorRowFieldText
	}
	return r.Field
}

// EffectiveReason is the row's reason with the default: invalid on a C2 row, none elsewhere.
func (r *ErrorRow) EffectiveReason() string {
	if r.Reason == "" && r.Class == ErrorClassC2 {
		return ErrorReasonInvalid
	}
	return r.Reason
}

// EffectiveWrite is the row's write verdict with the default: may_have_run on a row whose scope
// isn't read; a read row has none.
func (r *ErrorRow) EffectiveWrite() string {
	if r.EffectiveScope() == ErrorRowScopeRead {
		return ""
	}
	if r.Write == "" {
		return ErrorWriteMayHaveRun
	}
	return r.Write
}

// AppliesToInterface reports whether the row reads replies on apiInterface: a row with no
// interface reads none (the field is required; the validator says so).
func (r *ErrorRow) AppliesToInterface(apiInterface string) bool {
	for _, declared := range r.Interface {
		if declared == apiInterface {
			return true
		}
	}
	return false
}

// AppliesToMethod reports whether the row reads replies to method: every method when the row
// names none.
func (r *ErrorRow) AppliesToMethod(method string) bool {
	if len(r.Methods) == 0 {
		return true
	}
	for _, m := range r.Methods {
		if m == method {
			return true
		}
	}
	return false
}

// ErrorShape says where a chain's errors sit in its replies when its node software doesn't use the
// interface's standard place (PRD §2.4). Pointers are RFC 6901 JSON Pointers into the reply body.
type ErrorShape struct {
	ID        string   `json:"id"`
	Interface string   `json:"interface,omitempty"`
	Methods   []string `json:"methods,omitempty"`
	// Code points at the chain's code; with Space, the code is "<space>/<code>" (Cosmos "sdk/32").
	Code  string `json:"code,omitempty"`
	Space string `json:"space,omitempty"`
	// Message points at the message; absent, the interface's standard text stands.
	Message string `json:"message,omitempty"`
	// When, for a 2xx (gRPC OK) reply, says the reply is an error inside a success. Absent, the
	// shape reads only replies that are already errors.
	When     *ShapeWhen `json:"when,omitempty"`
	Source   string     `json:"source,omitempty"`
	Disabled bool       `json:"disabled,omitempty"`
	Note     string     `json:"note,omitempty"`
}

// AppliesToMethod reports whether the shape reads replies to method: every method when it names
// none (a body check applies only to the methods it was written for, R-B).
func (s *ErrorShape) AppliesToMethod(method string) bool {
	if len(s.Methods) == 0 {
		return true
	}
	for _, m := range s.Methods {
		if m == method {
			return true
		}
	}
	return false
}

// ShapeWhen is a shape's condition on a successful reply: the value At the pointer Equals, or is
// Not, a JSON value. Exactly one of the two is set; raw, matched by JSON type and value.
type ShapeWhen struct {
	At     string          `json:"at"`
	Equals json.RawMessage `json:"equals,omitempty"`
	Not    json.RawMessage `json:"not,omitempty"`
}

// InterfaceList is a row's interface: one name or a list of them in the file, a list in Go.
type InterfaceList []string

// UnmarshalJSON accepts "jsonrpc" and ["jsonrpc", "rest"] alike.
func (l *InterfaceList) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*l = InterfaceList{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return fmt.Errorf("interface: a name or a list of names, got %s", data)
	}
	*l = InterfaceList(many)
	return nil
}

// MarshalJSON writes one name bare and several as a list, the way the files read.
func (l InterfaceList) MarshalJSON() ([]byte, error) {
	if len(l) == 1 {
		return json.Marshal(l[0])
	}
	return json.Marshal([]string(l))
}
