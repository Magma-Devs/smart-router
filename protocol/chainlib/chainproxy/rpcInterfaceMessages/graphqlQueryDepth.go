package rpcInterfaceMessages

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/lexer"
	gqlparser "github.com/vektah/gqlparser/v2/parser"
)

// MaxGraphQLQueryDepth is how deeply a GraphQL document may nest, counting every bracket that opens
// a level: selection sets, argument lists, list and object values, list types.
//
// gqlparser parses by recursion and sets no depth limit of its own, and a goroutine that runs out
// of stack ends the process: that is a fatal error, not a panic a recover() can catch. The limit
// keeps the parse to a small stack whatever the document holds (MAG-4359).
//
// It refuses nothing a node answers: Sui caps a query's depth at 20 (serviceConfig maxQueryDepth)
// and answers a selection nested 200 deep with GRAPHQL_PARSE_FAILED "recursion limit exceeded".
const MaxGraphQLQueryDepth = 256

// parseGraphQLQuery parses a GraphQL document once it is known to nest no deeper than
// MaxGraphQLQueryDepth.
//
// The two failures it returns are different answers, and the caller must not merge them: a document
// past the depth limit is a refusal the ROUTER made (*GraphQLRefusal), while a document the parser
// cannot read is the document's own fault. Wrapping the first in "failed parsing graphql query"
// made the router's own limit look like a parse failure, which is both the wrong message for the
// caller and the wrong classification for the forward-the-unreadable direction MAG-4351 decided: a
// document too deep for the router's parser is exactly what must not be parsed.
func parseGraphQLQuery(query string) (*ast.QueryDocument, error) {
	source := &ast.Source{Input: query}
	if err := checkGraphQLQueryDepth(source); err != nil {
		return nil, err
	}
	doc, err := gqlparser.ParseQuery(source)
	if err != nil {
		return nil, fmt.Errorf("failed parsing graphql query: %w", err)
	}
	return doc, nil
}

// checkGraphQLQueryDepth refuses a document that nests deeper than MaxGraphQLQueryDepth. It reads
// the document with gqlparser's own lexer, which loops rather than recurses, so a bracket inside a
// string, a block string or a comment is not counted.
//
// The count matches the parser's recursion up to the first closing bracket of the wrong kind, and
// the parser stops at that bracket with a syntax error. A document the lexer cannot read is left to
// the parser, which stops at the same place with its own message.
func checkGraphQLQueryDepth(source *ast.Source) error {
	tokens := lexer.New(source)
	depth := 0
	for {
		token, err := tokens.ReadToken()
		if err != nil {
			return nil //nolint:nilerr // the parser stops at the same token and reports it in its own words
		}
		switch token.Kind {
		case lexer.EOF:
			return nil
		case lexer.BraceL, lexer.BracketL, lexer.ParenL:
			depth++
			if depth > MaxGraphQLQueryDepth {
				// A refusal, not a parse error: the document is readable, the router declines to
				// read it. The position is kept in the message, because it is the one thing that
				// tells the caller where to cut (MAG-4577).
				return RefuseGraphQLRequest("graphql query nests deeper than %d levels (line %d, column %d)",
					MaxGraphQLQueryDepth, token.Pos.Line, token.Pos.Column)
			}
		case lexer.BraceR, lexer.BracketR, lexer.ParenR:
			if depth > 0 {
				depth--
			}
		}
	}
}
