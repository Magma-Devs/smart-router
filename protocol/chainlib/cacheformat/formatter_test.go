package cacheformat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// MAG-3611: the id belongs to the caller. A reply served from the cache must carry
// back the id the caller sent, byte for byte, whatever its JSON type. Keeping the raw
// text is the only way to do that: decoding a text id and re-encoding it wrapped it
// in a second pair of quotation marks, and decoding a number as an integer rounded a
// fractional or oversized one. The cache key, meanwhile, must not see the id at all.
func TestOutputFormatterRestoresTheCallersIDByteForByte(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{"text id", `"abc"`},
		{"text id with an escaped quote", `"a\"b"`},
		{"number id", `80001`},
		{"fractional number id", `1.5`},
		{"number id beyond int64", `123456789012345678901234567890`},
		{"null id", `null`},
	}
	var keyedForms [][]byte
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":"eth_blockNumber","params":[]}`, tc.id))
			inputFormatter, outputFormatter := FormatterForRelayRequestAndResponseJsonRPC()

			keyed := inputFormatter(request)
			require.Equal(t, "1", gjson.GetBytes(keyed, IDFieldName).Raw, "the cache key must see the same fixed id for every caller")
			keyedForms = append(keyedForms, keyed)

			reply := outputFormatter([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`))
			require.True(t, gjson.ValidBytes(reply), "restoring the id must leave valid JSON: %s", reply)
			require.Equal(t, tc.id, gjson.GetBytes(reply, IDFieldName).Raw, "the caller gets back exactly the id it sent")
			require.Equal(t, `"0x64"`, gjson.GetBytes(reply, "result").Raw, "nothing but the id changes")
		})
	}
	for i := 1; i < len(keyedForms); i++ {
		require.Equal(t, string(keyedForms[0]), string(keyedForms[i]), "requests that differ only by id must key identically")
	}
}

func TestOutputFormatterRestoresEachIDOfABatch(t *testing.T) {
	inputFormatter, outputFormatter := FormatterForRelayRequestAndResponseJsonRPC()
	keyed := inputFormatter([]byte(`[{"jsonrpc":"2.0","id":"first","method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]},{"jsonrpc":"2.0","id":null,"method":"net_version","params":[]}]`))
	for _, element := range gjson.ParseBytes(keyed).Array() {
		require.Equal(t, "1", element.Get(IDFieldName).Raw, "every element keys under the fixed id")
	}

	reply := outputFormatter([]byte(`[{"jsonrpc":"2.0","id":1,"result":"0x64"},{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":1,"result":"1"}]`))
	require.True(t, gjson.ValidBytes(reply))
	restored := gjson.ParseBytes(reply).Array()
	require.Len(t, restored, 3)
	require.Equal(t, `"first"`, restored[0].Get(IDFieldName).Raw)
	require.Equal(t, `2`, restored[1].Get(IDFieldName).Raw)
	require.Equal(t, `null`, restored[2].Get(IDFieldName).Raw)
}

// A notification carries no id. Its reply restores a null id, as it did before ids
// were kept as raw text; the point of this case is that "no id" never produces the
// invalid `"id":` a raw-text restore of an empty string would.
func TestOutputFormatterRestoresNullForARequestWithoutAnID(t *testing.T) {
	inputFormatter, outputFormatter := FormatterForRelayRequestAndResponseJsonRPC()
	keyed := inputFormatter([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}`))
	require.Equal(t, "1", gjson.GetBytes(keyed, IDFieldName).Raw)

	reply := outputFormatter([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`))
	require.True(t, gjson.ValidBytes(reply), "%s", reply)
	require.Equal(t, "null", gjson.GetBytes(reply, IDFieldName).Raw)
}
