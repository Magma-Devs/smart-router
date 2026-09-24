package lavasession

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// frameReader hands out at most 16 KB per Read, the size of an HTTP/2 DATA frame, then ends with
// err, or io.EOF when err is nil.
type frameReader struct {
	data []byte
	off  int
	err  error
}

func (r *frameReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	n := copy(p[:min(len(p), 16<<10)], r.data[r.off:])
	r.off += n
	return n, nil
}

func responseWithBody(body []byte, contentLength int64, endErr error) *http.Response {
	return &http.Response{
		ContentLength: contentLength,
		Body:          io.NopCloser(&frameReader{data: body, err: endErr}),
	}
}

// MAG-3845: a body whose length the upstream announced is read into one buffer of that size;
// everything else is read as before.
func TestReadResponseBody(t *testing.T) {
	block := bytes.Repeat([]byte(`{"a":1}`), 150_000) // about 1 MB, many 16 KB frames

	t.Run("an announced length is read into one buffer of that size", func(t *testing.T) {
		body, err := readResponseBody(responseWithBody(block, int64(len(block)), nil))
		require.NoError(t, err)
		require.Equal(t, block, body)
		require.Equal(t, len(block)+1, cap(body), "the buffer must be the announced size and never grow")
	})

	t.Run("no announced length is read as before", func(t *testing.T) {
		body, err := readResponseBody(responseWithBody(block, -1, nil))
		require.NoError(t, err)
		require.Equal(t, block, body)
	})

	t.Run("an empty body", func(t *testing.T) {
		body, err := readResponseBody(responseWithBody(nil, 0, nil))
		require.NoError(t, err)
		require.Empty(t, body)
	})

	t.Run("an announcement above the ceiling is not allocated up front", func(t *testing.T) {
		reply := []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)
		body, err := readResponseBody(responseWithBody(reply, maxPresizedResponseBytes+1, nil))
		require.NoError(t, err)
		require.Equal(t, reply, body)
		require.Less(t, cap(body), maxPresizedResponseBytes, "an announced length above the ceiling must not be allocated")
	})

	t.Run("a body longer than announced is read whole", func(t *testing.T) {
		body, err := readResponseBody(responseWithBody(block, 100, nil))
		require.NoError(t, err)
		require.Equal(t, block, body)
	})

	t.Run("a body shorter than announced keeps the transport's error", func(t *testing.T) {
		_, err := readResponseBody(responseWithBody(block[:1000], int64(len(block)), io.ErrUnexpectedEOF))
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
}

// Both read sites, against a real server: one reply with a Content-Length, and one streamed in
// chunks with none.
func TestHTTPDirectRPCConnection_ReadsAnnouncedAndStreamedBodies(t *testing.T) {
	payload := bytes.Repeat([]byte(`{"k":"v"}`), 120_000) // about 1 MB
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/streamed" {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = w.Write(payload)
			return
		}
		// Flushing before the handler returns makes the server send chunks with no length.
		flusher, _ := w.(http.Flusher)
		for off := 0; off < len(payload); off += 64 << 10 {
			_, _ = w.Write(payload[off:min(off+64<<10, len(payload))])
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, path := range []string{"/announced", "/streamed"} {
		t.Run(path, func(t *testing.T) {
			conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: srv.URL + path}, 1, "")
			require.NoError(t, err)
			h, ok := conn.(*HTTPDirectRPCConnection)
			require.True(t, ok, "conn must be *HTTPDirectRPCConnection")

			sent, err := h.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":1}`), nil)
			require.NoError(t, err)
			require.Equal(t, payload, sent.Data)

			done, err := h.DoHTTPRequest(ctx, HTTPRequestParams{Method: http.MethodGet, URL: srv.URL + path})
			require.NoError(t, err)
			require.Equal(t, payload, done.Body)
		})
	}
}

// BenchmarkReadResponseBody compares the two read paths on a 5.9 MB body delivered in 16 KB
// frames, the shape of an evening Solana block over HTTP/2. "unannounced" is io.ReadAll, which
// is how every body was read before MAG-3845.
//
//	go test ./protocol/lavasession/ -bench ReadResponseBody -benchmem -run=^$ -count=3
func BenchmarkReadResponseBody(b *testing.B) {
	payload := make([]byte, 5_900_000)
	for _, tc := range []struct {
		name          string
		contentLength int64
	}{
		{"announced", int64(len(payload))},
		{"unannounced", -1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := readResponseBody(responseWithBody(payload, tc.contentLength, nil)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
