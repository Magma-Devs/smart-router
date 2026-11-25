package common

import (
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The answer rows (PRD §2.2 `answer: true`; MAG-5595). An answer row reads a reply's answer rather
// than an error record: a success row (`write: took_it`) says the write's copy took it, a not-found
// row (`class: C3, reason: not_found`) says a null answer is a "not found" the router asks one more
// provider about (R-F). Both name their methods, and match the value `at` a JSON pointer by `code`
// (JSON type and value, null included) or by `present`, or match any answer to those methods.
//
// A success row with `at` reads a write copy's body under any status (PRD §3.2 step 2; R-L); a row
// without `at` reads a 2xx only. A not-found row reads a 2xx only: it is about an answer.

// GraphQLOccurrence is one root field of a GraphQL request as the reader found it: the field's name
// and the alias the reply carries it under ("" when none).
type GraphQLOccurrence struct {
	Field string
	Alias string
}

// MatchAnswerRow is the first of rows that reads the reply: an answer row for apiInterface and method,
// of a scope the request's kind allows (isWrite), whose `at` check holds on doc, the decoded reply.
// success says the reply's status was a 2xx (gRPC OK). ok is false when no row reads it.
func MatchAnswerRow(rows []*spectypes.ErrorRow, apiInterface, method string, isWrite, success bool, doc any) (*spectypes.ErrorRow, bool) {
	for _, row := range rows {
		if !answerRowReads(row, apiInterface, method, isWrite, success) {
			continue
		}
		if row.At == "" || atMatches(row, doc) {
			return row, true
		}
	}
	return nil, false
}

// MatchAnswerRowPerOccurrence is MatchAnswerRow for a GraphQL reply, read once per root field of the
// request (R-O): each occurrence is read with its alias replaced by the field's name, so a row's `at`
// of /data/transaction reads {"data": {"tx": null}} when the request asked `tx: transaction(...)`,
// and the same field asked twice under two aliases is read twice. The result is one row per
// occurrence that a row reads, in the request's order, with the occurrence it read.
func MatchAnswerRowPerOccurrence(rows []*spectypes.ErrorRow, occurrences []GraphQLOccurrence, isWrite, success bool, doc any) []AnswerRowMatch {
	var matches []AnswerRowMatch
	for _, occurrence := range occurrences {
		view := graphQLOccurrenceView(doc, occurrence)
		if row, ok := MatchAnswerRow(rows, spectypes.APIInterfaceGraphQL, occurrence.Field, isWrite, success, view); ok {
			matches = append(matches, AnswerRowMatch{Row: row, Occurrence: occurrence})
		}
	}
	return matches
}

// AnswerRowMatch is a row that read one occurrence of a GraphQL root field.
type AnswerRowMatch struct {
	Row        *spectypes.ErrorRow
	Occurrence GraphQLOccurrence
}

// answerRowReads reports whether row is an answer row for this reply, before its `at` check.
func answerRowReads(row *spectypes.ErrorRow, apiInterface, method string, isWrite, success bool) bool {
	if row == nil || row.Disabled || !row.Answer || !row.AppliesToInterface(apiInterface) || !row.AppliesToMethod(method) {
		return false
	}
	switch row.EffectiveScope() {
	case spectypes.ErrorRowScopeWrite:
		if !isWrite {
			return false
		}
	case spectypes.ErrorRowScopeRead:
		if isWrite {
			return false
		}
	}
	if success {
		return true
	}
	// Under a status that isn't a 2xx only a success row with `at` reads a write copy's body.
	return isWrite && row.At != "" && row.EffectiveWrite() == spectypes.ErrorWriteTookIt
}

// atMatches reports whether the value at the row's pointer satisfies its `present` or `code`.
func atMatches(row *spectypes.ErrorRow, doc any) bool {
	value, found := ResolveJSONPointer(doc, row.At)
	if row.Present != nil {
		return (found && value != nil) == *row.Present
	}
	if row.HasCode() {
		return found && JSONValueEquals(value, row.Code)
	}
	return found
}

// graphQLOccurrenceView is doc with the occurrence's alias read as its field name: /data/<field> is
// the value the reply carries at /data/<alias>, and an error whose path starts at the alias has the
// field's name there. Other root fields' entries are left as they are; a reply that is not an object
// is returned as is.
func graphQLOccurrenceView(doc any, occurrence GraphQLOccurrence) any {
	if occurrence.Alias == "" || occurrence.Alias == occurrence.Field {
		return doc
	}
	root, ok := doc.(map[string]any)
	if !ok {
		return doc
	}
	view := make(map[string]any, len(root))
	for key, value := range root {
		view[key] = value
	}
	if data, ok := root["data"].(map[string]any); ok {
		renamed := make(map[string]any, len(data))
		for key, value := range data {
			if key == occurrence.Alias {
				renamed[occurrence.Field] = value
				continue
			}
			if key == occurrence.Field {
				// The field's own entry would mask the alias's; the occurrence under its name is
				// another occurrence, read on its own.
				continue
			}
			renamed[key] = value
		}
		view["data"] = renamed
	}
	if errs, ok := root["errors"].([]any); ok {
		view["errors"] = renameErrorPaths(errs, occurrence)
	}
	return view
}

// renameErrorPaths is errs with an error path that starts at the occurrence's alias starting at the
// field's name instead.
func renameErrorPaths(errs []any, occurrence GraphQLOccurrence) []any {
	renamed := make([]any, 0, len(errs))
	for _, entry := range errs {
		errObj, ok := entry.(map[string]any)
		if !ok {
			renamed = append(renamed, entry)
			continue
		}
		path, ok := errObj["path"].([]any)
		if !ok || len(path) == 0 || path[0] != occurrence.Alias {
			renamed = append(renamed, entry)
			continue
		}
		copied := make(map[string]any, len(errObj))
		for key, value := range errObj {
			copied[key] = value
		}
		newPath := append([]any{occurrence.Field}, path[1:]...)
		copied["path"] = newPath
		renamed = append(renamed, copied)
	}
	return renamed
}

// ReadGraphQLPathsAsFields gives each GraphQL error record whose path starts at an alias the request
// gave a root field the field's name there instead (PRD §2.2: a row's path names fields, never
// aliases), so an error at ["tx"] for `tx: transaction(...)` is read at ["transaction"]. A path
// starts at a response key, and response keys are unique in an operation, so each alias names one
// field. occurrences are the request's root fields (GraphQLMessage.Occurrences).
func ReadGraphQLPathsAsFields(records []ErrorRecord, occurrences []GraphQLOccurrence) {
	fieldOf := map[string]string{}
	for _, occurrence := range occurrences {
		if occurrence.Alias != "" && occurrence.Alias != occurrence.Field {
			fieldOf[occurrence.Alias] = occurrence.Field
		}
	}
	if len(fieldOf) == 0 {
		return
	}
	for i := range records {
		if records[i].Interface != spectypes.APIInterfaceGraphQL || len(records[i].Path) == 0 {
			continue
		}
		if field, aliased := fieldOf[records[i].Path[0]]; aliased {
			records[i].Path = append([]string{field}, records[i].Path[1:]...)
		}
	}
}
