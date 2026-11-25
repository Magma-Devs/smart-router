package rpcInterfaceMessages

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// twoWalkBatchElements is the batch walk the one-walk scanJsonrpcBatchEnvelopes replaces: it
// skips each element to find its end and leaves the envelope scan to the caller. It stays here
// as the reference the new walk is compared against.
func twoWalkBatchElements(data []byte, fn func(elementBytes []byte) bool) error {
	pos := SkipJSONWhitespace(data, 0)
	if pos >= len(data) {
		return fmt.Errorf("empty input")
	}
	if data[pos] != '[' {
		return fmt.Errorf("expected '[' at position %d, got %q", pos, data[pos])
	}
	pos++
	pos = SkipJSONWhitespace(data, pos)
	if pos < len(data) && data[pos] == ']' {
		return nil
	}
	for {
		if pos >= len(data) {
			return fmt.Errorf("truncated: expected element")
		}
		elementStart := pos
		elementEnd, err := skipValue(data, pos)
		if err != nil {
			return fmt.Errorf("element: %w", err)
		}
		if !fn(data[elementStart:elementEnd]) {
			return nil
		}
		pos = SkipJSONWhitespace(data, elementEnd)
		if pos >= len(data) {
			return fmt.Errorf("truncated: expected ',' or ']'")
		}
		if data[pos] == ']' {
			return nil
		}
		if data[pos] != ',' {
			return fmt.Errorf("expected ',' or ']' at position %d, got %q", pos, data[pos])
		}
		pos++
		pos = SkipJSONWhitespace(data, pos)
	}
}

// twoWalkBatchCheck is checkResponseErrorForBatch on the two-walk reference: elementCheck scans
// the element it is handed.
func twoWalkBatchCheck(data []byte, elementCheck func(element []byte) (bool, string)) (bool, string) {
	var (
		hasAnySuccess bool
		aggregated    strings.Builder
	)
	walkErr := twoWalkBatchElements(data, func(element []byte) bool {
		if elemHasErr, elemMsg := elementCheck(element); elemHasErr {
			if aggregated.Len() > 0 {
				aggregated.WriteString(",-,")
			}
			aggregated.WriteString(elemMsg)
			return true
		}
		hasAnySuccess = true
		return true
	})
	if walkErr != nil {
		return true, fmt.Sprintf("malformed JSON-RPC batch response: %v", walkErr)
	}
	if !BatchNodeErrorOnAny && hasAnySuccess {
		return false, ""
	}
	if aggregated.Len() == 0 {
		return false, ""
	}
	return true, aggregated.String()
}

func twoWalkJsonRpcBatch(data []byte) (bool, string) {
	return twoWalkBatchCheck(data, func(element []byte) (bool, string) {
		hasErr, msg, _ := checkJsonrpcEnvelope(element, "JSON-RPC batch element")
		return hasErr, msg
	})
}

func twoWalkTendermintBatch(data []byte) (bool, string) {
	return twoWalkBatchCheck(data, func(element []byte) (bool, string) {
		return TendermintrpcMessage{}.CheckResponseError(element, 200)
	})
}

func twoWalkIsJsonrpcReply(data []byte) bool {
	if scan, err := scanJsonrpcEnvelope(data); err == nil {
		return carriesJsonrpcAnswer(scan)
	}
	replies := 0
	err := twoWalkBatchElements(data, func(element []byte) bool {
		scan, err := scanJsonrpcEnvelope(element)
		if err != nil || !carriesJsonrpcAnswer(scan) {
			replies = -1
			return false
		}
		replies++
		return true
	})
	return err == nil && replies > 0
}

// batchSeeds are batch replies of every shape the checks tell apart, plus the shapes that break
// a scanner: brackets and quotes inside strings, escapes at the end of a string, an element that
// is balanced by its bracket count and wrong by its bracket kinds, and elements that are no object.
var batchSeeds = []string{
	`[]`,
	`[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`,
	`[{"jsonrpc":"2.0","id":1,"result":{"a":[1,2,{"b":null}],"c":"}]{["}},{"jsonrpc":"2.0","id":2,"result":[]}]`,
	`[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}]`,
	`[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"bad \"params\" ]}"}}]`,
	`[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"first"}},{"jsonrpc":"2.0","id":2,"error":{"code":-32001,"message":"second"}}]`,
	`[{"jsonrpc":"2.0","id":1,"error":null,"result":7},{"jsonrpc":"2.0","id":2,"error":{"code":0,"message":""},"result":8}]`,
	`[{"jsonrpc":"2.0","id":1},{"id":2,"result":null}]`,
	`[{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":""}}]`,
	`[{"jsonrpc":"2.0","id":1,"error":"not an object"},{"jsonrpc":"2.0","id":2,"error":5}]`,
	` [ {"jsonrpc" : "2.0" , "id" : 1 , "result" : { "k" : "v" } } , {"id":2,"result":true} ] `,
	`[{"id":1,"result":"a\\"},{"id":2,"result":"\\\"]"},{"id":3,"result":"]""}]`,
	`[{"id":1,"result":{"a":[1,2}}},{"id":2,"result":1}]`,
	`[{"id":1,"result":1},42,"text",null,true,[1,2],{"id":7,"result":2}]`,
	`[{"id":1,"result":{"response":{"code":5,"log":"app refused"}}},{"id":2,"result":{"response":{"code":0,"log":""}}}]`,
	`[{"id":1,"result":{"response":{"code":5,"log":"only fault"}}}]`,
	`[{"id":1,"result":{"code":3,"log":"broadcast refused","hash":"AB"}}]`,
	`[{"id":1,"result":tru},{"id":2,"result":-1.5e+3}]`,
	`[{"id":1,"result":1}{"id":2,"result":2}]`,
	`[{"id":1,"result":1},]`,
	`[{"id":1 "result":1}]`,
	`[{"id":1,"result":1}] trailing`,
	`{"jsonrpc":"2.0","id":1,"result":1}`,
	`<html><body>502 Bad Gateway</body></html>`,
	``,
}

// batchVariants returns seed and every reply one edit away from it: each prefix (a cut
// connection), each single byte dropped, and each byte replaced by one that changes structure.
func batchVariants(seed string) [][]byte {
	variants := [][]byte{[]byte(seed)}
	for cut := 0; cut < len(seed); cut++ {
		variants = append(variants, []byte(seed[:cut]))
		variants = append(variants, []byte(seed[:cut]+seed[cut+1:]))
		for _, replacement := range []byte(`{}[]",:\ x1`) {
			if seed[cut] != replacement {
				variants = append(variants, []byte(seed[:cut]+string(replacement)+seed[cut+1:]))
			}
		}
	}
	return variants
}

func requireSameAsTwoWalks(t testing.TB, data []byte) {
	t.Helper()
	wantErr, wantMsg := twoWalkJsonRpcBatch(data)
	gotErr, gotMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
	if wantErr != gotErr || wantMsg != gotMsg {
		t.Fatalf("JSON-RPC batch verdict moved for %q\n two walks: %v %q\n one walk:  %v %q", data, wantErr, wantMsg, gotErr, gotMsg)
	}
	wantErr, wantMsg = twoWalkTendermintBatch(data)
	gotErr, gotMsg = CheckResponseErrorForTendermintBatch(data, 200)
	if wantErr != gotErr || wantMsg != gotMsg {
		t.Fatalf("Tendermint batch verdict moved for %q\n two walks: %v %q\n one walk:  %v %q", data, wantErr, wantMsg, gotErr, gotMsg)
	}
	if want, got := twoWalkIsJsonrpcReply(data), IsJsonrpcReply(data); want != got {
		t.Fatalf("IsJsonrpcReply moved for %q: two walks %v, one walk %v", data, want, got)
	}
}

// The one-walk batch check returns the verdict and the message of the two-walk check, byte for
// byte, on every seed and every reply one edit away from one, under both aggregation modes.
func TestBatchCheck_OneWalkMatchesTwoWalks(t *testing.T) {
	previous := BatchNodeErrorOnAny
	defer func() { BatchNodeErrorOnAny = previous }()
	cases := 0
	for _, onAny := range []bool{false, true} {
		BatchNodeErrorOnAny = onAny
		for _, seed := range batchSeeds {
			for _, data := range batchVariants(seed) {
				requireSameAsTwoWalks(t, data)
				cases++
			}
		}
	}
	require.Greater(t, cases, 20000, "the corpus shrank")
}

// The walk hands fn, for every element, the scan scanJsonrpcEnvelope gives for that element's
// bytes, and the bytes the two-walk reference hands out.
func TestScanJsonrpcBatchEnvelopes_ScanIsTheElementsOwn(t *testing.T) {
	for _, seed := range batchSeeds {
		data := []byte(seed)
		var want [][]byte
		wantErr := twoWalkBatchElements(data, func(element []byte) bool {
			want = append(want, element)
			return true
		})
		index := 0
		gotErr := scanJsonrpcBatchEnvelopes(data, func(scan jsonrpcScanResult, scanErr error, element []byte) bool {
			require.Less(t, index, len(want), "more elements than the reference for %q", seed)
			require.Equal(t, string(want[index]), string(element), "element %d of %q", index, seed)
			ownScan, ownErr := scanJsonrpcEnvelope(element)
			if ownErr != nil {
				require.EqualError(t, scanErr, ownErr.Error(), "element %d of %q", index, seed)
			} else {
				require.NoError(t, scanErr, "element %d of %q", index, seed)
				require.Equal(t, ownScan.hasError, scan.hasError)
				require.Equal(t, ownScan.hasResult, scan.hasResult)
				require.Equal(t, string(ownScan.errorBytes), string(scan.errorBytes))
				require.Equal(t, string(ownScan.resultBytes), string(scan.resultBytes))
			}
			index++
			return true
		})
		if wantErr != nil {
			require.EqualError(t, gotErr, wantErr.Error(), "walk error for %q", seed)
			continue
		}
		require.NoError(t, gotErr, "walk error for %q", seed)
		require.Equal(t, len(want), index, "element count for %q", seed)
	}
}

// scanJsonrpcEnvelopePrefix stops at the object's closing brace and says where that is, whatever
// follows it.
func TestScanJsonrpcEnvelopePrefix_ReturnsTheEndOfTheObject(t *testing.T) {
	object := `{"jsonrpc":"2.0","id":1,"result":{"a":"}"}}`
	for _, tail := range []string{``, `,{"id":2}]`, ` ]`, `}`, `garbage`} {
		res, end, err := scanJsonrpcEnvelopePrefix([]byte(object + tail))
		require.NoError(t, err, "tail %q", tail)
		require.Equal(t, len(object), end, "tail %q", tail)
		require.True(t, res.hasResult)
		require.Equal(t, `{"a":"}"}`, string(res.resultBytes))
	}
	_, end, err := scanJsonrpcEnvelopePrefix([]byte(`  {}  `))
	require.NoError(t, err)
	require.Equal(t, 4, end)
}

func FuzzBatchCheck_OneWalkMatchesTwoWalks(f *testing.F) {
	for _, seed := range batchSeeds {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		requireSameAsTwoWalks(t, data)
	})
}

// blockLikeBatch builds a batch reply shaped like three block replies: megabytes of short
// strings and small objects under each element's result.
func blockLikeBatch(elements, transactionsPerElement int) []byte {
	var b bytes.Buffer
	b.WriteByte('[')
	for e := 0; e < elements; e++ {
		if e > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"jsonrpc":"2.0","id":%d,"result":{"blockHeight":%d,"transactions":[`, e+1, 300000000+e)
		for i := 0; i < transactionsPerElement; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"meta":{"err":null,"fee":5000,"logMessages":["Program 11111111111111111111111111111111 invoke [1]","Program log: \"ok\" %d"],"postBalances":[%d,1,2039280]},"transaction":{"signatures":["5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQU%d"],"message":{"accountKeys":["Vote111111111111111111111111111111111111111","SysvarC1ock11111111111111111111111111111111"],"recentBlockhash":"EkSnNWid2cvwEVnVx9aBqawnmiCNiDgp3gUdkDPTKN1N"}}}`, i, i*7, i)
		}
		b.WriteString(`]}}`)
	}
	b.WriteByte(']')
	return b.Bytes()
}

func BenchmarkBatchCheck_BlockLikeReply(b *testing.B) {
	data := blockLikeBatch(3, 3500)
	if hasErr, msg := CheckResponseErrorForJsonRpcBatch(data, 200); hasErr {
		b.Fatal(msg)
	}
	b.Run("one_walk", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			CheckResponseErrorForJsonRpcBatch(data, 200)
		}
	})
	b.Run("two_walks", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			twoWalkJsonRpcBatch(data)
		}
	})
}
