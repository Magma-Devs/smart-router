package lavasession

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/gzip"
)

// gzipReaders keeps inflaters for reuse. An upstream that gzips does it on every
// reply, the tiny ones too (tatum compresses a 44-byte getSlot answer), and building
// a reader allocates ~37 KB and takes several times longer than inflating a reply
// that size.
var gzipReaders sync.Pool

// readHTTPResponseBody reads resp's body, and inflates it when the upstream gzipped
// it. net/http will not: it decodes only the gzip it asked for itself, and never
// asks once the request carries an Accept-Encoding. A body left as it arrived goes
// through readResponseBody, which presizes it from its Content-Length (MAG-3845).
//
// Decoding follows the reply's Content-Encoding, not the request's Accept-Encoding.
// The header is what says what the bytes are, and an upstream that gzips despite
// identity would otherwise hand the relay path a body it cannot parse.
//
// Inflating drops Content-Encoding and Content-Length from resp.Header, as net/http
// does when it decodes. The headers travel with the reply to the client, and have to
// describe the bytes returned rather than the ones on the wire.
func readHTTPResponseBody(resp *http.Response) ([]byte, error) {
	if !isGzipContentEncoding(resp.Header) {
		return readResponseBody(resp)
	}
	body, err := gunzip(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("inflating gzip reply: %w", err)
	}
	resp.Header.Del("Content-Encoding")
	resp.Header.Del("Content-Length")
	return body, nil
}

// isGzipContentEncoding reports whether the reply's only content coding is gzip.
// x-gzip is the alias RFC 9110 says to treat as gzip. Stacked codings ("gzip, br",
// or two header lines) are left encoded, as every reply was before the router could
// inflate one.
func isGzipContentEncoding(header http.Header) bool {
	codings := header.Values("Content-Encoding")
	if len(codings) != 1 {
		return false
	}
	coding := strings.TrimSpace(codings[0])
	return strings.EqualFold(coding, "gzip") || strings.EqualFold(coding, "x-gzip")
}

// gunzip inflates a whole gzip stream, concatenated members included, as it is
// read: the compressed bytes are never held whole. Content-Length counts the
// compressed bytes, so unlike an identity reply the inflated body is not presized,
// and io.ReadAll grows it. An empty stream is an empty body (a 204, or the reply to
// a HEAD). A truncated stream or a checksum mismatch is an error.
func gunzip(r io.Reader) ([]byte, error) {
	zr, ok := gzipReaders.Get().(*gzip.Reader)
	if !ok {
		zr = new(gzip.Reader)
	}
	defer gzipReaders.Put(zr)
	if err := zr.Reset(r); err != nil {
		if err == io.EOF {
			return []byte{}, nil
		}
		return nil, err
	}
	return io.ReadAll(zr)
}
