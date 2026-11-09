package rpcInterfaceMessages

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// graphQLBody wraps a document in a GraphQL-over-HTTP request body.
func graphQLBody(document string) []byte {
	return []byte(`{"query":"` + document + `"}`)
}

// fragmentDoubling is a document whose fragments each spread the next one twice, so it resolves to
// 2^levels root fields in a body that grows by one fragment per level.
func fragmentDoubling(levels int) string {
	var document strings.Builder
	document.WriteString("{ ...F0 } ")
	for level := 0; level < levels; level++ {
		fmt.Fprintf(&document, "fragment F%d on Query { ...F%d ...F%d } ", level, level+1, level+1)
	}
	fmt.Fprintf(&document, "fragment F%d on Query { chainIdentifier }", levels)
	return document.String()
}

// fragmentChain is a document that spreads the first of a chain of fragments spreads times, each
// fragment spreading the next and the last selecting one field. Every spread is expanded through
// the whole chain.
func fragmentChain(spreads, length int) string {
	var document strings.Builder
	document.WriteString("{ " + strings.Repeat("...F1 ", spreads) + "} ")
	for link := 1; link < length; link++ {
		fmt.Fprintf(&document, "fragment F%d on Query { ...F%d } ", link, link+1)
	}
	fmt.Fprintf(&document, "fragment F%d on Query { chainIdentifier }", length)
	return document.String()
}

func TestParseGraphQLMsgRootFieldLimit(t *testing.T) {
	message, err := ParseGraphQLMsg(graphQLBody("{ " + strings.Repeat("chainIdentifier ", MaxGraphQLRootFields) + "}"))
	require.NoError(t, err)
	require.Len(t, message.RootFields, MaxGraphQLRootFields)

	refusal := fmt.Sprintf("graphql operation selects more than %d root fields", MaxGraphQLRootFields)
	_, err = ParseGraphQLMsg(graphQLBody("{ " + strings.Repeat("chainIdentifier ", MaxGraphQLRootFields+1) + "}"))
	require.ErrorContains(t, err, refusal)

	// Fragments count by what they expand to: 2^9 root fields from ten small fragments.
	_, err = ParseGraphQLMsg(graphQLBody(fragmentDoubling(9)))
	require.ErrorContains(t, err, refusal)

	// The same fragment spread under several root fields still counts once per spread.
	_, err = ParseGraphQLMsg(graphQLBody("{ " + strings.Repeat("...F ", MaxGraphQLRootFields/2+1) + "} fragment F on Query { a b }"))
	require.ErrorContains(t, err, refusal)
}

func TestParseGraphQLMsgFragmentNestingLimit(t *testing.T) {
	refusal := fmt.Sprintf("graphql fragments nest more than %d deep", MaxGraphQLFragmentNesting)

	message, err := ParseGraphQLMsg(graphQLBody(fragmentChain(1, MaxGraphQLFragmentNesting)))
	require.NoError(t, err)
	require.Equal(t, "chainIdentifier", message.GetMethod())
	_, err = ParseGraphQLMsg(graphQLBody(fragmentChain(1, MaxGraphQLFragmentNesting+1)))
	require.ErrorContains(t, err, refusal)

	// An inline fragment nests the same way a spread does.
	inline := func(depth int) string {
		return "{ " + strings.Repeat("... on Query { ", depth) + "chainIdentifier" + strings.Repeat(" }", depth) + " }"
	}
	_, err = ParseGraphQLMsg(graphQLBody(inline(MaxGraphQLFragmentNesting)))
	require.NoError(t, err)
	_, err = ParseGraphQLMsg(graphQLBody(inline(MaxGraphQLFragmentNesting + 1)))
	require.ErrorContains(t, err, refusal)
}

// TestParseGraphQLMsgAmplifiedDocumentsFinish sends two small documents that expand to far more
// than they hold: fragments that double 24 times, and a 10,000-link fragment chain spread at 300
// roots (MAG-4360). Each must come back with an error, and come back at all, which is what the
// deadline checks: it is far past what a bounded expansion takes, so it does not time a fast
// machine against a slow one.
func TestParseGraphQLMsgAmplifiedDocumentsFinish(t *testing.T) {
	tests := []struct {
		name     string
		document string
		refusal  string
	}{
		{
			name:     "fragments that double 24 times",
			document: fragmentDoubling(24),
			refusal:  fmt.Sprintf("graphql operation selects more than %d root fields", MaxGraphQLRootFields),
		},
		{
			name:     "a long fragment chain spread at every root",
			document: fragmentChain(MaxGraphQLRootFields, 10_000),
			refusal:  fmt.Sprintf("graphql fragments nest more than %d deep", MaxGraphQLFragmentNesting),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				_, err := ParseGraphQLMsg(graphQLBody(test.document))
				done <- err
			}()
			select {
			case err := <-done:
				require.ErrorContains(t, err, test.refusal)
			case <-time.After(10 * time.Second):
				t.Fatalf("parsing a %d-byte request did not finish in 10s: its expansion is unbounded", len(test.document))
			}
		})
	}
}

func TestParseGraphQLMsgFirstFragmentDefinitionWins(t *testing.T) {
	// Indexing fragments by name keeps FragmentDefinitionList.ForName's answer for a name defined
	// twice, which a schema-validating server refuses but this parser accepts.
	message, err := ParseGraphQLMsg(graphQLBody("{ ...F } fragment F on Query { checkpoint } fragment F on Query { epoch }"))
	require.NoError(t, err)
	require.Equal(t, "checkpoint", message.GetMethod())
}
