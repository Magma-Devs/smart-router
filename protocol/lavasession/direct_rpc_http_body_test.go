package lavasession

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// gzipBytes compresses with the standard library on purpose: the reader under test
// is klauspost's, and a reply from an upstream was not written by it either.
func gzipBytes(t testing.TB, plain []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(plain)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func responseWith(body []byte, header http.Header) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func TestReadHTTPResponseBody(t *testing.T) {
	plain := []byte(`{"jsonrpc":"2.0","id":1,"result":{"slot":449994332}}`)
	compressed := gzipBytes(t, plain)

	t.Run("an unencoded reply comes back as it arrived, headers included", func(t *testing.T) {
		header := http.Header{"Content-Type": {"application/json"}, "Content-Length": {strconv.Itoa(len(plain))}}
		body, err := readHTTPResponseBody(responseWith(plain, header))
		require.NoError(t, err)
		require.Equal(t, plain, body)
		require.Equal(t, strconv.Itoa(len(plain)), header.Get("Content-Length"))
	})

	for _, coding := range []string{"gzip", "GZIP", " gzip ", "x-gzip"} {
		t.Run(fmt.Sprintf("Content-Encoding %q is inflated and its framing headers dropped", coding), func(t *testing.T) {
			header := http.Header{
				"Content-Type":     {"application/json"},
				"Content-Encoding": {coding},
				"Content-Length":   {strconv.Itoa(len(compressed))},
				"X-Request-Id":     {"abc"},
			}
			body, err := readHTTPResponseBody(responseWith(compressed, header))
			require.NoError(t, err)
			require.Equal(t, plain, body)
			// These travel to the client with the inflated body; kept, they would
			// label plain JSON as gzip and give it the compressed length.
			require.Empty(t, header.Values("Content-Encoding"))
			require.Empty(t, header.Values("Content-Length"))
			require.Equal(t, "application/json", header.Get("Content-Type"), "other headers are the upstream's and stay")
			require.Equal(t, "abc", header.Get("X-Request-Id"))
		})
	}

	t.Run("concatenated gzip members inflate to all of them", func(t *testing.T) {
		second := []byte(`{"jsonrpc":"2.0","id":2,"result":null}`)
		stream := append(gzipBytes(t, plain), gzipBytes(t, second)...)
		body, err := readHTTPResponseBody(responseWith(stream, http.Header{"Content-Encoding": {"gzip"}}))
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, plain...), second...), body)
	})

	t.Run("an empty gzip reply is an empty body, not an error", func(t *testing.T) {
		header := http.Header{"Content-Encoding": {"gzip"}, "Content-Length": {"0"}}
		body, err := readHTTPResponseBody(responseWith(nil, header))
		require.NoError(t, err)
		require.Empty(t, body)
		require.Empty(t, header.Values("Content-Encoding"))
	})

	for name, coding := range map[string][]string{
		"brotli":                      {"br"},
		"stacked codings":             {"gzip, br"},
		"two Content-Encoding fields": {"gzip", "br"},
	} {
		t.Run(name+" is left encoded", func(t *testing.T) {
			header := http.Header{"Content-Encoding": coding}
			body, err := readHTTPResponseBody(responseWith(compressed, header))
			require.NoError(t, err)
			require.Equal(t, compressed, body, "only a lone gzip coding is decoded")
			require.Equal(t, coding, header.Values("Content-Encoding"), "the bytes are still encoded, so the header stays true")
		})
	}

	corrupt := map[string][]byte{
		"not gzip at all":   []byte(`{"jsonrpc":"2.0"}`),
		"truncated stream":  compressed[:len(compressed)/2],
		"checksum mismatch": flipByte(compressed, len(compressed)-8), // the CRC-32 in the trailer
	}
	for name, body := range corrupt {
		t.Run(name+" is an error, not a body", func(t *testing.T) {
			_, err := readHTTPResponseBody(responseWith(body, http.Header{"Content-Encoding": {"gzip"}}))
			require.Error(t, err)
			require.Contains(t, err.Error(), "inflating gzip reply")
		})
	}
}

func flipByte(in []byte, at int) []byte {
	out := append([]byte{}, in...)
	out[at] ^= 0xff
	return out
}

// Readers go back to the pool after every use, failed ones included. A reader that
// carried state from its previous reply into the next would corrupt or reject it.
func TestReadHTTPResponseBody_PooledReadersDoNotLeakBetweenReplies(t *testing.T) {
	const workers, rounds = 16, 50

	// Distinct lengths and contents per reply, so a reader reused with a stale
	// window, length or checksum cannot pass by coincidence. Built up front: the
	// workers must not call require.
	type reply struct{ plain, compressed []byte }
	replies := make([][]reply, workers)
	for w := range replies {
		replies[w] = make([]reply, rounds)
		for r := range replies[w] {
			plain := bytes.Repeat([]byte(fmt.Sprintf(`{"worker":%d,"round":%d}`, w, r)), 1+(w*rounds+r)%97)
			replies[w][r] = reply{plain: plain, compressed: gzipBytes(t, plain)}
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range replies {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r, reply := range replies[w] {
				if r%10 == 0 {
					// A failed inflate returns its reader to the pool too.
					truncated := reply.compressed[:len(reply.compressed)-3]
					if _, err := readHTTPResponseBody(responseWith(truncated, http.Header{"Content-Encoding": {"gzip"}})); err == nil {
						errs <- fmt.Errorf("worker %d round %d: truncated reply inflated without error", w, r)
						return
					}
				}
				body, err := readHTTPResponseBody(responseWith(reply.compressed, http.Header{"Content-Encoding": {"gzip"}}))
				if err != nil {
					errs <- fmt.Errorf("worker %d round %d: %w", w, r, err)
					return
				}
				if !bytes.Equal(reply.plain, body) {
					errs <- fmt.Errorf("worker %d round %d: inflated %d bytes, want %d", w, r, len(body), len(reply.plain))
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// BenchmarkReadHTTPResponseBody measures what reading a reply costs with and without
// gzip, for the two shapes an opted-in Solana url sees: the small replies an
// upstream like tatum gzips anyway, and a block.
//
//	go test ./protocol/lavasession/ -run '^$' -bench ReadHTTPResponseBody -benchmem -count=6
func BenchmarkReadHTTPResponseBody(b *testing.B) {
	for _, tc := range []struct {
		name  string
		plain []byte
	}{
		{"getSlot", []byte(`{"id":1,"jsonrpc":"2.0","result":449994332}`)},
		{"getBlock_2.4MB", syntheticBlockReply(2_400_000)},
	} {
		compressed := gzipBytes(b, tc.plain)
		b.Run(tc.name+"/identity", func(b *testing.B) {
			b.SetBytes(int64(len(tc.plain)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := readHTTPResponseBody(responseWith(tc.plain, http.Header{})); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/gzip", func(b *testing.B) {
			b.ReportMetric(float64(len(tc.plain))/float64(len(compressed)), "ratio")
			b.SetBytes(int64(len(tc.plain)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := readHTTPResponseBody(responseWith(compressed, http.Header{"Content-Encoding": {"gzip"}})); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// syntheticBlockReply builds a deterministic getBlock-like reply of at least
// targetBytes: transactions whose signatures are unique and whose account keys
// repeat across the block, which is what makes a real block compress the way it does.
func syntheticBlockReply(targetBytes int) []byte {
	rng := rand.New(rand.NewPCG(3844, 3844))
	const base58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	randomKey := func(n int) string {
		key := make([]byte, n)
		for i := range key {
			key[i] = base58[rng.IntN(len(base58))]
		}
		return string(key)
	}
	accounts := make([]string, 4096)
	for i := range accounts {
		accounts[i] = randomKey(44)
	}

	var buf bytes.Buffer
	buf.WriteString(`{"jsonrpc":"2.0","id":1,"result":{"blockHeight":427519402,"transactions":[`)
	for i := 0; buf.Len() < targetBytes; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"transaction":{"signatures":["%s"],"message":{"accountKeys":[`, randomKey(88))
		for k := 0; k < 8; k++ {
			if k > 0 {
				buf.WriteByte(',')
			}
			fmt.Fprintf(&buf, `"%s"`, accounts[rng.IntN(len(accounts))])
		}
		fmt.Fprintf(&buf, `]}},"meta":{"err":null,"fee":5000,"preBalances":[%d,%d],"postBalances":[%d,%d],`+
			`"logMessages":["Program ComputeBudget111111111111111111111111111111 invoke [1]","Program ComputeBudget111111111111111111111111111111 success"],`+
			`"computeUnitsConsumed":%d},"version":0}`,
			rng.Int64N(1e12), rng.Int64N(1e12), rng.Int64N(1e12), rng.Int64N(1e12), rng.IntN(1_400_000))
	}
	buf.WriteString(`]}}`)
	return buf.Bytes()
}
