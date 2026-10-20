package rpcInterfaceMessages

import "github.com/vektah/gqlparser/v2/ast"

// MaxGraphQLRootFields is how many root fields one GraphQL request may resolve to once its fragments
// are expanded. Every root field runs chainlib's whole per-operation pipeline (api lookup, headers,
// block parsing, a longer combined method name), and fragment spreads multiply root fields, so
// without a limit the cost of a request grew with what its fragments expand to rather than with its
// size (MAG-4360).
//
// 300 is Sui's own ceiling on the fields of one query (serviceConfig maxQueryNodes).
const MaxGraphQLRootFields = 300

// MaxGraphQLFragmentNesting is how deeply fragment spreads and inline fragments may nest where the
// router expands them, at the root of the operation. A fragment is walked again each time it is
// spread, so the length of a chain of spreads multiplies with the number of spreads. With this limit
// and MaxGraphQLRootFields the expansion visits a bounded number of selections whatever the document
// holds (MAG-4360).
const MaxGraphQLFragmentNesting = 32

// graphQLFragmentsByName indexes a document's fragments by name. The expansion looks a fragment up
// on every spread, and FragmentDefinitionList.ForName searches the whole list each time. The first
// definition of a name wins, as in ForName.
func graphQLFragmentsByName(definitions ast.FragmentDefinitionList) map[string]*ast.FragmentDefinition {
	byName := make(map[string]*ast.FragmentDefinition, len(definitions))
	for _, definition := range definitions {
		if _, defined := byName[definition.Name]; !defined {
			byName[definition.Name] = definition
		}
	}
	return byName
}
