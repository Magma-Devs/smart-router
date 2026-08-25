package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/goccy/go-json"
)

// BenchmarkExtractBlockHeightFromEVMResponse measures the EVM fallback on the
// response shapes it actually parses. "decode" is the interface{} decode of
// the whole body that the gjson path lookup replaced.
func BenchmarkExtractBlockHeightFromEVMResponse(b *testing.B) {
	var logs strings.Builder
	logs.WriteString(`{"jsonrpc":"2.0","id":1,"result":[`)
	for i := range 1000 {
		if i > 0 {
			logs.WriteByte(',')
		}
		logs.WriteString(`{"address":"0x` + strings.Repeat("ab", 20) + `","blockNumber":"0x12a7b5c","data":"0x` + strings.Repeat("cd", 64) + `","topics":["0x` + strings.Repeat("ef", 32) + `","0x` + strings.Repeat("12", 32) + `"],"transactionHash":"0x` + strings.Repeat("34", 32) + `","logIndex":"0x1"}`)
	}
	logs.WriteString(`]}`)

	var block strings.Builder
	block.WriteString(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x12a7b5c","hash":"0x` + strings.Repeat("ab", 32) + `","transactions":[`)
	for i := range 300 {
		if i > 0 {
			block.WriteByte(',')
		}
		block.WriteString(`{"hash":"0x` + strings.Repeat("ef", 32) + `","from":"0x` + strings.Repeat("12", 20) + `","input":"0x` + strings.Repeat("34", 60) + `","value":"0x0"}`)
	}
	block.WriteString(`]}}`)

	cases := []struct {
		name   string
		method string
		body   []byte
		want   int64
	}{
		{"eth_getLogs_1000", "eth_getLogs", []byte(logs.String()), 0x12a7b5c},
		{"eth_getBlockByNumber_300tx", "eth_getBlockByNumber", []byte(block.String()), 0x12a7b5c},
		{"eth_blockNumber", "eth_blockNumber", []byte(`{"jsonrpc":"2.0","id":1,"result":"0x12a7b5c"}`), 0x12a7b5c},
	}
	for _, tc := range cases {
		b.Run(tc.name+"/gjson", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := extractBlockHeightFromEVMResponse(tc.body, tc.method); got != tc.want {
					b.Fatalf("got %d want %d", got, tc.want)
				}
			}
		})
		b.Run(tc.name+"/decode", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var resp struct {
					Result any `json:"result"`
				}
				if err := json.Unmarshal(tc.body, &resp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
