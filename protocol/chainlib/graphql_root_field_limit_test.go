package chainlib

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
)

// TestGraphQLChainParserRefusesAmplifiedRequests sends ParseMsg a 675-byte request whose 16
// fragments each spread the next one twice, so it expands to 65,536 root fields (MAG-4360). It is
// refused before any root field reaches the per-operation pipeline, and a request at the limit is
// still served.
func TestGraphQLChainParserRefusesAmplifiedRequests(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	var document strings.Builder
	document.WriteString("{ ...F0 } ")
	for level := 0; level < 16; level++ {
		fmt.Fprintf(&document, "fragment F%d on Query { ...F%d ...F%d } ", level, level+1, level+1)
	}
	document.WriteString("fragment F16 on Query { chainIdentifier }")
	body := []byte(`{"query":"` + document.String() + `"}`)
	require.Len(t, body, 675)

	_, err := chainParser.ParseMsg("", body, graphqlConnectionType, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.ErrorContains(t, err, fmt.Sprintf("graphql operation selects more than %d root fields", rpcInterfaceMessages.MaxGraphQLRootFields))

	atLimit := []byte(`{"query":"{ ` + strings.Repeat("chainIdentifier ", rpcInterfaceMessages.MaxGraphQLRootFields) + `}"}`)
	chainMessage, err := chainParser.ParseMsg("", atLimit, graphqlConnectionType, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	require.Len(t, strings.Split(chainMessage.GetApi().Name, rpcInterfaceMessages.GraphQLMethodSeparator), rpcInterfaceMessages.MaxGraphQLRootFields)
}
