package chainlib

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// rawBytesCodec sends a []byte as the message body, so the probe below needs no proto type.
type rawBytesCodec struct{}

func (rawBytesCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("rawBytesCodec: cannot marshal %T", v)
	}
	return b, nil
}

func (rawBytesCodec) Unmarshal(data []byte, v any) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawBytesCodec: cannot unmarshal into %T", v)
	}
	*p = append([]byte(nil), data...)
	return nil
}

func (rawBytesCodec) Name() string { return "mag3881-raw" }

// TestGRPCIncomingMetadata_ValuesOutliveTheConnection pins the premise the gRPC listener rests on
// (MAG-3881): a value read from metadata.FromIncomingContext is the router's to keep. fiber's header
// strings point into a request buffer the next request overwrites; grpc-go's do not, because the
// transport decodes every header value into its own string (x/net's hpack decodeString allocates,
// for the literal and the Huffman encoding alike, and a -bin value is base64-decoded into a fresh
// one). A real server keeps the first request's value while the same connection carries more
// requests with same-length values, is closed, and a new connection carries another.
//
// What a black-box test can show is that the value survives; that nothing could have overwritten it
// is the decoder's source. Both of the decoder's branches are exercised: the client encodes a header
// value with Huffman coding when that is shorter and as a literal otherwise, and the two controls
// below pin which value takes which path.
func TestGRPCIncomingMetadata_ValuesOutliveTheConnection(t *testing.T) {
	const probeHeader = "x-lava-probe"
	var mu sync.Mutex
	var seen []string
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(srv any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		mu.Lock()
		seen = append(seen, md.Get(probeHeader)...) // the metadata's own strings, not copies
		mu.Unlock()
		return status.Error(codes.Unimplemented, "probe")
	}))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	dial := func() *grpc.ClientConn {
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		return conn
	}
	call := func(conn *grpc.ClientConn, value string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, probeHeader, value)
		var reply []byte
		err := conn.Invoke(ctx, "/mag3881.Probe/Echo", []byte("probe"), &reply, grpc.ForceCodec(rawBytesCodec{}))
		require.Equal(t, codes.Unimplemented, status.Code(err), "the probe must reach the handler: %v", err)
	}

	huffman := "tests.simulator.simulator_production_tests"
	literal := "{^~|`}{^~|`}{^~|`}{^~|`}{^~|`}"
	require.Less(t, hpack.HuffmanEncodeLength(huffman), uint64(len(huffman)), "control: this value is sent Huffman-coded")
	require.GreaterOrEqual(t, hpack.HuffmanEncodeLength(literal), uint64(len(literal)), "control: this value is sent as a literal")

	first := dial()
	call(first, huffman)
	call(first, literal)
	// The same connection, so the same transport read buffers, now carries values of the same
	// lengths. A kept value that pointed into those buffers would read as one of these.
	var later []string
	for i := 0; i < 8; i++ {
		for _, value := range []string{strings.Repeat("x", len(huffman)), strings.Repeat("`", len(literal))} {
			later = append(later, value)
			call(first, value)
		}
	}
	require.NoError(t, first.Close())
	second := dial()
	defer second.Close()
	fresh := strings.Repeat("z", len(huffman))
	call(second, fresh)
	runtime.GC()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, append(append([]string{huffman, literal}, later...), fresh), seen, "every request's value must be kept as it was sent")
}
