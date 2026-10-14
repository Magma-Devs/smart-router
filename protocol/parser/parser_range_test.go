package parser

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestParseBlockFromParams_RangeLowerBound pins PARSER_TYPE_BLOCK_EARLIEST (MAG-4242 item 12).
// The Ethereum-family specs put it on trace_filter's fromBlock, but it had no implementation:
// parseGeneric answered "unsupported generic parser type" and the lower bound of a range was never
// read, so a request reaching deep into history was judged on its toBlock alone.
func TestParseBlockFromParams_RangeLowerBound(t *testing.T) {
	fromBlock := spectypes.GenericParser{ParsePath: ".params.[0].fromBlock", ParseType: spectypes.PARSER_TYPE_BLOCK_EARLIEST}
	toBlock := spectypes.GenericParser{ParsePath: ".params.[0].toBlock", ParseType: spectypes.PARSER_TYPE_BLOCK_LATEST}
	// trace_filter's legacy block parsing in the spec: DEFAULT latest.
	defaultLatest := spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT}
	filter := func(fields map[string]interface{}) RPCInput {
		return &RPCInputTest{Params: []interface{}{fields}}
	}

	tests := []struct {
		name         string
		params       map[string]interface{}
		parsers      []spectypes.GenericParser
		wantBlock    int64
		wantEarliest int64
		wantHasRange bool
	}{
		{
			name:    "from and to, the earliest parser listed first",
			params:  map[string]interface{}{"fromBlock": "0x10", "toBlock": "latest"},
			parsers: []spectypes.GenericParser{fromBlock, toBlock}, wantBlock: spectypes.LATEST_BLOCK, wantEarliest: 16, wantHasRange: true,
		},
		{
			name:    "from and to, the earliest parser listed last",
			params:  map[string]interface{}{"fromBlock": "0x10", "toBlock": "latest"},
			parsers: []spectypes.GenericParser{toBlock, fromBlock}, wantBlock: spectypes.LATEST_BLOCK, wantEarliest: 16, wantHasRange: true,
		},
		{
			name:    "a numeric upper bound stays the main block",
			params:  map[string]interface{}{"fromBlock": "0x10", "toBlock": "0x20"},
			parsers: []spectypes.GenericParser{fromBlock, toBlock}, wantBlock: 32, wantEarliest: 16, wantHasRange: true,
		},
		{
			name:    "from only: the main block falls back to the legacy default, the bound is kept",
			params:  map[string]interface{}{"fromBlock": "0x10"},
			parsers: []spectypes.GenericParser{fromBlock, toBlock}, wantBlock: spectypes.LATEST_BLOCK, wantEarliest: 16, wantHasRange: true,
		},
		{
			name:    "to only: no range",
			params:  map[string]interface{}{"toBlock": "0x20"},
			parsers: []spectypes.GenericParser{fromBlock, toBlock}, wantBlock: 32, wantHasRange: false,
		},
		{
			name:    "the earliest tag is the chain's first block",
			params:  map[string]interface{}{"fromBlock": "earliest", "toBlock": "latest"},
			parsers: []spectypes.GenericParser{fromBlock, toBlock}, wantBlock: spectypes.LATEST_BLOCK, wantEarliest: spectypes.EARLIEST_BLOCK, wantHasRange: true,
		},
		{
			name:    "an unparseable bound is dropped, the main block still decides",
			params:  map[string]interface{}{"fromBlock": "zzz", "toBlock": "0x20"},
			parsers: []spectypes.GenericParser{fromBlock, toBlock}, wantBlock: 32, wantHasRange: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed := ParseBlockFromParams(filter(tc.params), defaultLatest, tc.parsers)
			require.Equal(t, tc.wantBlock, parsed.GetBlock())
			earliest, hasRange := parsed.GetEarliestBlock()
			require.Equal(t, tc.wantHasRange, hasRange)
			if tc.wantHasRange {
				require.Equal(t, tc.wantEarliest, earliest)
			}
		})
	}
}
