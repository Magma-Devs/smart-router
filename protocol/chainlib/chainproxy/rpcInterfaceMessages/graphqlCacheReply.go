package rpcInterfaceMessages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// GraphQLReplyCacheExclusion names why a GraphQL reply must not be written to the cache, or returns
// "" when it may be (MAG-5123, GraphQL chapter section 6).
//
// The cache keeps only a clean, complete answer that every provider would give. Three replies are
// not one, though each is a 200 that passes the node-error gate:
//
//   - A reply with any error, a partial reply (data beside errors) included. A caller's error is
//     not a node fault, so it reaches the client as the node sent it; cached, it would answer every
//     caller with one provider's verdict on a document another provider's version may accept, and
//     a partial reply would hold back the part a provider that has it could give.
//   - A root field that is null or an empty list, the "not found" null included. A lagging provider
//     answers transaction(digest) with null for a transaction it has not indexed yet; cached, every
//     caller would read "never ran" for that digest, and some would sign again (MAG-5201). An empty
//     list is the same answer for a list read.
//   - A body that is not a GraphQL response at all, which the node-error gate already refuses
//     (MAG-4369). It is named here too, so this rule does not depend on that one.
//
// Every member of data is read, whatever its name: a reply names a root field by its alias when
// the request gives one. Only the root level is read. A null deeper down is a field of an answer,
// such as an epoch that has not ended yet.
func GraphQLReplyCacheExclusion(data []byte) string {
	var reply struct {
		Data   map[string]json.RawMessage `json:"data"`
		Errors []json.RawMessage          `json:"errors"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return "the reply is not a GraphQL response"
	}
	if len(reply.Errors) > 0 {
		return "the reply carries errors"
	}
	if reply.Data == nil {
		return "the reply carries no data"
	}
	keys := make([]string, 0, len(reply.Data))
	for key := range reply.Data {
		keys = append(keys, key)
	}
	// Sorted, so a reply with several empty root fields always names the same one in the log.
	sort.Strings(keys)
	for _, key := range keys {
		value := bytes.TrimSpace(reply.Data[key])
		if bytes.Equal(value, []byte("null")) {
			return fmt.Sprintf("root field %q is null", key)
		}
		if graphQLValueIsEmptyList(value) {
			return fmt.Sprintf("root field %q is an empty list", key)
		}
	}
	return ""
}

// graphQLValueIsEmptyList reports whether a raw JSON value is a list with no element.
func graphQLValueIsEmptyList(value []byte) bool {
	if len(value) < 2 || value[0] != '[' || value[len(value)-1] != ']' {
		return false
	}
	return len(bytes.TrimSpace(value[1:len(value)-1])) == 0
}
