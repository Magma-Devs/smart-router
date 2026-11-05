package parser

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-4758: an EIP-1898 block object names its block as {"blockNumber": "0x…"}. parseByArg must
// read that height, not render the object as "map[blockNumber:0x…]".
func TestParseByArg_EIP1898BlockObject(t *testing.T) {
	byArg1 := spectypes.BlockParser{ParserArg: []string{"1"}, ParserFunc: spectypes.PARSER_FUNC_PARSE_BY_ARG}
	address := "0x1111111111111111111111111111111111111111"

	tests := []struct {
		name     string
		block    interface{}
		expected int64
	}{
		{"hex number", "0x10", 16},
		{"blockNumber object", map[string]interface{}{"blockNumber": "0x10"}, 16},
		{"blockNumber object with requireCanonical", map[string]interface{}{"blockNumber": "0x10", "requireCanonical": true}, 16},
		{"blockNumber object with a tag", map[string]interface{}{"blockNumber": "latest"}, spectypes.LATEST_BLOCK},
		// No height in the request: still NOT_APPLICABLE, routing for it is MAG-4142.
		{"blockHash object", map[string]interface{}{"blockHash": "0xabc"}, spectypes.NOT_APPLICABLE},
		{"blockNumber that is not a string", map[string]interface{}{"blockNumber": 16.0}, spectypes.NOT_APPLICABLE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rpcInput := &MockRPCInput{params: []interface{}{address, tt.block}}
			require.Equal(t, tt.expected, ParseBlockFromParams(rpcInput, byArg1, nil).GetBlock())
		})
	}
}
