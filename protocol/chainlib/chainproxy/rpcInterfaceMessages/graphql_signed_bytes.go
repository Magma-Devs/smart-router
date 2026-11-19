package rpcInterfaceMessages

import (
	"regexp"
	"strconv"
	"strings"
)

// graphQLSignedBytesRun is a run of base64 text longer than 66 characters: the signed bytes a GraphQL
// write carries, in the encoding the chain's list names for them (MAG-4909, Sui data 4: base64). A
// JSON-escaped slash ("\/") is part of the run, so a client that escapes slashes does not split the
// bytes into pieces short enough to stay.
//
// 66 characters is the longest id the logs must keep: a Sui address or object id is 0x and 64 hex
// digits, and a digest is 44 base58 characters at most. The shortest signed bytes are longer: a Sui
// signature is 97 bytes, 132 characters in base64. Hex is part of the base64 alphabet, so a
// transaction a chain signs in 0x-hex is cut by the same rule, as Part I R-L cuts a 0x-hex run longer
// than 66 characters.
var graphQLSignedBytesRun = regexp.MustCompile(`(?:[A-Za-z0-9+/=]|\\/){67,}`)

// RedactGraphQLSignedBytes replaces the signed bytes in text, a GraphQL request or reply a log line
// carries, with [REDACTED:<n>], n the number of characters taken out (MAG-5122; Part I R-L logs,
// Part II Appendix C item 5). Redact before cutting a text to a log's length: a cut that lands inside
// the bytes can leave a piece too short to match.
//
// A run of letters alone is a word, not bytes, and stays: base64 of 50 bytes or more holds a digit,
// a + or a / all but always (all letters by chance: about once in a million at 67 characters, once
// in 10^12 at a signature's 132).
func RedactGraphQLSignedBytes(text string) string {
	if len(text) < 67 {
		return text
	}
	return graphQLSignedBytesRun.ReplaceAllStringFunc(text, func(run string) string {
		if !strings.ContainsAny(run, "0123456789+/=") {
			return run
		}
		return "[REDACTED:" + strconv.Itoa(len(run)) + "]"
	})
}

// RedactSignedBytes is RedactGraphQLSignedBytes, for the log lines that hold the request as a
// message rather than as the GraphQL interface.
func (gm *GraphQLMessage) RedactSignedBytes(text string) string {
	return RedactGraphQLSignedBytes(text)
}
