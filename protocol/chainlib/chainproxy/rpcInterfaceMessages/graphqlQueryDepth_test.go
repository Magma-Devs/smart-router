package rpcInterfaceMessages

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

// nestedSelection is a query whose selections nest depth levels deep, counting the operation's
// own selection set.
func nestedSelection(depth int) string {
	return "{ " + strings.Repeat("a{", depth-1) + "b" + strings.Repeat("}", depth-1) + " }"
}

func TestCheckGraphQLQueryDepth(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		refused bool
	}{
		{name: "selections at the limit", query: nestedSelection(MaxGraphQLQueryDepth)},
		{name: "selections one past the limit", query: nestedSelection(MaxGraphQLQueryDepth + 1), refused: true},
		{
			// An argument list, a list value and an object value each open a level, the same as
			// a selection set does: the parser recurses into every one of them.
			name:  "arguments and values at the limit",
			query: "{ " + strings.Repeat("a{", MaxGraphQLQueryDepth-4) + "b(x: [{y: 1}])" + strings.Repeat("}", MaxGraphQLQueryDepth-4) + " }",
		},
		{
			name:    "arguments and values one past the limit",
			query:   "{ " + strings.Repeat("a{", MaxGraphQLQueryDepth-3) + "b(x: [{y: 1}])" + strings.Repeat("}", MaxGraphQLQueryDepth-3) + " }",
			refused: true,
		},
		{
			name:  "list types in variable definitions",
			query: "query($v: " + strings.Repeat("[", MaxGraphQLQueryDepth-1) + "Int" + strings.Repeat("]", MaxGraphQLQueryDepth-1) + ") { b }",
		},
		{
			name:    "list types one past the limit",
			query:   "query($v: " + strings.Repeat("[", MaxGraphQLQueryDepth) + "Int" + strings.Repeat("]", MaxGraphQLQueryDepth) + ") { b }",
			refused: true,
		},
		{name: "brackets in a string are not nesting", query: `{ a(s: "` + strings.Repeat("{[(", 1000) + `") }`},
		{name: "brackets in a block string are not nesting", query: `{ a(s: """` + strings.Repeat("{[(", 1000) + `""") }`},
		{name: "brackets in a comment are not nesting", query: "{ a # " + strings.Repeat("{[(", 1000) + "\n }"},
		{
			// Stray closers end the parse with a syntax error; they must not bank depth the
			// check then lets a later run of openers spend.
			name:    "stray closers do not offset later nesting",
			query:   "{ a }" + strings.Repeat("}", 100) + nestedSelection(MaxGraphQLQueryDepth+1),
			refused: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := checkGraphQLQueryDepth(&ast.Source{Input: test.query})
			if test.refused {
				require.ErrorContains(t, err, fmt.Sprintf("query nests deeper than %d levels", MaxGraphQLQueryDepth))
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestParseGraphQLMsgRefusesNestingPastTheLimit(t *testing.T) {
	message, err := ParseGraphQLMsg([]byte(`{"query":"` + nestedSelection(MaxGraphQLQueryDepth) + `"}`))
	require.NoError(t, err)
	require.Equal(t, "a", message.GetMethod())

	_, err = ParseGraphQLMsg([]byte(`{"query":"` + nestedSelection(MaxGraphQLQueryDepth+1) + `"}`))
	require.ErrorContains(t, err, "failed parsing graphql query")
	require.ErrorContains(t, err, fmt.Sprintf("query nests deeper than %d levels", MaxGraphQLQueryDepth))
}

const deepNestingChildEnv = "GRAPHQL_DEEP_NESTING_CHILD"

// TestParseGraphQLMsgDeepNestingLeavesTheProcessUp parses a document nested far past the limit
// (MAG-4359). Without the limit the parse runs the goroutine out of stack, which ends the process
// instead of returning, so it runs in a child process, and the test asserts that the child gets an
// ordinary error, parses the next request and exits cleanly.
func TestParseGraphQLMsgDeepNestingLeavesTheProcessUp(t *testing.T) {
	const survived = "parsed the next request after the deeply nested one"
	if os.Getenv(deepNestingChildEnv) == "1" {
		const depth = 600_000
		body := `{"query":"` + nestedSelection(depth) + `"}`
		require.Greater(t, len(body), 1_800_000)

		_, err := ParseGraphQLMsg([]byte(body))
		require.ErrorContains(t, err, fmt.Sprintf("query nests deeper than %d levels", MaxGraphQLQueryDepth))

		message, err := ParseGraphQLMsg([]byte(`{"query":"{ chainIdentifier }"}`))
		require.NoError(t, err)
		require.Equal(t, "chainIdentifier", message.GetMethod())
		fmt.Println(survived)
		return
	}

	child := exec.Command(os.Args[0], "-test.run=^TestParseGraphQLMsgDeepNestingLeavesTheProcessUp$", "-test.v")
	child.Env = append(os.Environ(), deepNestingChildEnv+"=1")
	output, err := child.CombinedOutput()
	if len(output) > 4096 {
		output = output[:4096]
	}
	require.NoError(t, err, "the process parsing the request did not survive it:\n%s", output)
	require.Contains(t, string(output), survived)
}
