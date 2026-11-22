package rpcInterfaceMessages

import "github.com/vektah/gqlparser/v2/ast"

// A mutation can select one executable root plus __typename. GraphQL merges repeated selections
// of that root, including child fields contributed by other fragments.
type graphQLMutationField struct {
	GraphQLRootField
	children map[string]string // response key to schema field name, for immediate children only
}

// IsNoopMutation reports whether the parsed operation selected only __typename or skipped every
// executable root. A provider can still answer the GraphQL request, but it received no transaction.
func (gm *GraphQLMessage) IsNoopMutation() bool {
	return gm != nil && gm.mutationField != nil && gm.mutationField.Name == ""
}

func (m *graphQLMutationField) collect(root GraphQLRootField, selections ast.SelectionSet, fragments map[string]*ast.FragmentDefinition, variables map[string]interface{}, visited map[string]bool) {
	if m.Name == "" {
		m.GraphQLRootField = root
		m.children = make(map[string]string)
	}
	if m.ResponseKey != root.ResponseKey || m.Name != root.Name {
		return // a second root is refused by refuseMultiRootMutation
	}

	// Visit a named fragment once across all occurrences of this root. Conditions use the
	// operation's resolved variables, so repeating an included fragment adds the same fields.
	pending := []ast.SelectionSet{selections}
	for len(pending) > 0 {
		last := len(pending) - 1
		current := pending[last]
		pending = pending[:last]
		for _, selection := range current {
			switch s := selection.(type) {
			case *ast.Field:
				if !graphQLSelectionIsLeftOut(s.Directives, variables) {
					m.children[graphQLResponseKey(s)] = s.Name
				}
			case *ast.InlineFragment:
				if !graphQLSelectionIsLeftOut(s.Directives, variables) {
					pending = append(pending, s.SelectionSet)
				}
			case *ast.FragmentSpread:
				fragment := fragments[s.Name]
				if fragment != nil && !visited[s.Name] && !graphQLSelectionIsLeftOut(s.Directives, variables) {
					visited[s.Name] = true
					pending = append(pending, fragment.SelectionSet)
				}
			}
		}
	}
}
