package grpcproxy

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ProxyCallBack = func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error)

// StreamProxyCallBack is the server-streaming counterpart of ProxyCallBack. It is
// called before the unary callback for every incoming request.
//
// Returning a nil *StreamResponse together with a nil error means "not a
// server-streaming method" — the proxy then serves the call through the unary
// ProxyCallBack, so wiring streaming up leaves ordinary unary traffic untouched.
type StreamProxyCallBack = func(ctx context.Context, method string, reqBody []byte) (*StreamResponse, error)

// StreamResponse is the producer half of a server-streaming call, handed back by a
// StreamProxyCallBack for the proxy to pump at the client.
type StreamResponse struct {
	// Replies carries already-encoded response messages, each one a marshalled
	// instance of the method's output type. The producer closes the channel when
	// the upstream stream ends; the proxy then closes the client stream with OK,
	// or with the status Err reports.
	Replies <-chan []byte

	// Err, when set, is called once after Replies has closed. A non-nil error ends
	// the client stream with that status instead of OK: the router gave the stream
	// up rather than seeing it end, and the client should not take it for a normal end.
	Err func() error

	// Metadata is sent as response headers ahead of the first message. This is
	// where the router-assigned subscription id belongs: a gRPC stream has no room
	// for an out-of-band acknowledgement frame, because every message on the wire
	// must decode as the method's output type.
	Metadata metadata.MD

	// Close is invoked exactly once when the client stream ends for any reason —
	// client cancellation, upstream EOF, or a failed send. It must drop this client
	// from the upstream subscription; without it a disconnected client leaves the
	// upstream stream running with nobody reading it.
	Close func()
}

type HealthReporter interface {
	IsHealthy() bool
}

// CallGate is asked about every call the proxy relays, with its method ("service/method") and
// before any of its messages is read. A non-nil error refuses the call with that status, so a
// client stream or a two-way stream is answered at once, even when its client sends nothing until
// it gets a reply (MAG-5133).
type CallGate = func(ctx context.Context, method string) error

// ProxyOption configures NewGRPCProxyWithReflection.
type ProxyOption func(*proxyOptions)

type proxyOptions struct {
	callGate CallGate
}

// WithCallGate has the proxy ask gate about every call it relays before reading it.
func WithCallGate(gate CallGate) ProxyOption {
	return func(o *proxyOptions) { o.callGate = gate }
}

// NewGRPCProxyWithReflection creates a gRPC proxy. A separate gRPC server answers
// reflection, v1 and v1alpha, from reflectionSource, which is what lets tools like
// grpcurl work against the router; without a source it answers for the reflection
// services alone. Either way, no reflection request reaches cb. The same server answers
// grpc.health.v1 from healthReporter, as the HTTP health check at healthCheckPath does.
//
// streamCallback is optional; when non-nil the proxy offers every request to it
// first, so server-streaming methods are served as streams instead of being forced
// through the single-message unary path.
func NewGRPCProxyWithReflection(cb ProxyCallBack, healthCheckPath string, cmdFlags common.ConsumerCmdFlags, healthReporter HealthReporter, reflectionSource ReflectionSource, streamCallback StreamProxyCallBack, opts ...ProxyOption) (*grpc.Server, *http.Server, error) {
	var options proxyOptions
	for _, opt := range opts {
		opt(&options)
	}
	serverReceiveMaxMessageSize := grpc.MaxRecvMsgSize(common.GRPCMaxCallRecvMsgSize) // 512 MB instead of grpc-go's 4 MB default
	s := grpc.NewServer(grpc.UnknownServiceHandler(makeProxyFunc(cb, streamCallback, options.callGate)), grpc.ForceServerCodec(RawBytesCodec{}), serverReceiveMaxMessageSize)

	wrappedServer := grpcweb.WrapServer(s)

	// The router's own services, reflection and health, get a server of their own with the
	// standard protobuf codec: the main server's RawBytesCodec cannot read their messages.
	if reflectionSource == nil {
		reflectionSource = noUpstreamReflection{}
	}
	ownServicesGrpcServer := grpc.NewServer(serverReceiveMaxMessageSize)
	RegisterReflection(ownServicesGrpcServer, reflectionSource)
	grpc_health_v1.RegisterHealthServer(ownServicesGrpcServer, &listenerHealth{reporter: healthReporter})
	wrappedOwnServicesServer := grpcweb.WrapServer(ownServicesGrpcServer)

	handler := func(resp http.ResponseWriter, req *http.Request) {
		// Set CORS headers
		resp.Header().Set("Access-Control-Allow-Origin", cmdFlags.OriginFlag)

		if req.Method == http.MethodOptions {
			resp.Header().Set("Access-Control-Allow-Methods", cmdFlags.MethodsFlag)
			// Empty cors-headers flag (the default) would echo an empty allow-list and
			// make the browser reject any preflight carrying a non-simple header. Treat
			// empty as "*" to match the flag's help text and the wild-card origin above.
			allowHeaders := cmdFlags.HeadersFlag
			if allowHeaders == "" {
				allowHeaders = "*"
			}
			resp.Header().Set("Access-Control-Allow-Headers", allowHeaders)
			resp.Header().Set("Access-Control-Allow-Credentials", cmdFlags.CredentialsFlag)
			resp.Header().Set("Access-Control-Max-Age", cmdFlags.CDNCacheDuration)
			resp.WriteHeader(fiber.StatusNoContent)
			resp.Write(make([]byte, 0))
			return
		}

		if healthReporter != nil && req.URL.Path == healthCheckPath && req.Method == http.MethodGet {
			if healthReporter.IsHealthy() {
				resp.WriteHeader(fiber.StatusOK)
				resp.Write([]byte("Healthy"))
			} else {
				resp.WriteHeader(fiber.StatusServiceUnavailable)
				resp.Write([]byte("Unhealthy"))
			}

			return
		}

		// Reflection and health are answered by the router itself.
		if isReflectionRequest(req.URL.Path) || isHealthRequest(req.URL.Path) {
			wrappedOwnServicesServer.ServeHTTP(resp, req)
			return
		}

		wrappedServer.ServeHTTP(resp, req)
	}

	httpServer := &http.Server{
		Handler: h2c.NewHandler(http.HandlerFunc(handler), &http2.Server{}),
	}

	return s, httpServer, nil
}

func makeProxyFunc(callBack ProxyCallBack, streamCallBack StreamProxyCallBack, callGate CallGate) grpc.StreamHandler {
	return func(srv interface{}, stream grpc.ServerStream) error {
		// currently the callback function does not account for headers.
		methodName, ok := grpc.MethodFromServerStream(stream)
		if !ok {
			return status.Error(codes.Unavailable, "unable to get method name")
		}
		method := methodName[1:] // strip first '/' of the method name
		if callGate != nil {
			if err := callGate(stream.Context(), method); err != nil {
				return err
			}
		}
		var reqBytes []byte
		err := stream.RecvMsg(&reqBytes)
		if err != nil {
			return err
		}

		if streamCallBack != nil {
			streamResponse, streamErr := streamCallBack(stream.Context(), method, reqBytes)
			if streamErr != nil {
				return streamErr
			}
			if streamResponse != nil {
				return serveServerStream(stream, streamResponse)
			}
			// nil response with nil error: not a server-streaming method. Fall through
			// to the unary path, which is the whole of the pre-streaming behaviour.
		}

		// The router relays one request message per call. A client that sends a second one is
		// streaming its requests, which the router does not serve, and an answer to the first
		// alone would drop the rest without a word (MAG-5133). The read runs beside the relay, so
		// a single-message call waits for nothing extra: its client closed its side with the
		// message, and the read ends at once.
		sentMore := make(chan bool, 1)
		go func() {
			var next []byte
			sentMore <- stream.RecvMsg(&next) == nil
		}()

		respBytes, md, err := callBack(stream.Context(), method, reqBytes)

		md = lowercaseMetadata(md)

		if clientSentMore(sentMore) {
			return status.Error(codes.Unimplemented, common.UnservedGRPCCallError.Error())
		}

		if err != nil {
			// On error the handler returns no message, so gRPC sends a trailers-only response — SetHeader
			// metadata is unreliable there, but trailers are always flushed with the status. Attach the
			// reply metadata (e.g. lava-cross-validation-* failure headers) as trailers so error responses
			// still carry it, matching the success path's SetHeader. Skipped when empty, so non-CV errors
			// are unchanged.
			if len(md) > 0 {
				stream.SetTrailer(md)
			}
			return err
		}

		if err := stream.SetHeader(md); err != nil {
			utils.FormatError("Got error when setting header", err, utils.LogAttr("headers", utils.RedactHeaders(md)))
		}
		return stream.SendMsg(respBytes)
	}
}

// secondMessageWait bounds how long a reply waits for the read beside it, when that read has not
// ended by the time the reply is ready: a client that has neither closed its side nor sent more.
var secondMessageWait = 50 * time.Millisecond

// clientSentMore reports whether the read beside the relay found a second request message. A read
// still open when the reply is ready gets secondMessageWait to end: a single-message call ends it
// at once, and a client streaming its requests sends its next one.
func clientSentMore(sentMore <-chan bool) bool {
	select {
	case more := <-sentMore:
		return more
	default:
	}
	timer := time.NewTimer(secondMessageWait)
	defer timer.Stop()
	select {
	case more := <-sentMore:
		return more
	case <-timer.C:
		return false
	}
}

// serveServerStream holds the client stream open and forwards upstream messages onto
// it until the upstream ends or the client goes away. This is the listener half the
// Direct RPC gRPC path was missing: the subscription manager could already open an
// upstream stream and fan it out to per-client channels, but nothing on the serving
// side kept a client connection alive to read one (MAG-2643).
//
// Returns nil on upstream end-of-stream, which closes the client stream with OK, unless
// response.Err reports why the stream was given up instead.
func serveServerStream(stream grpc.ServerStream, response *StreamResponse) error {
	// Runs on every exit path, so a client that disconnects mid-stream is dropped
	// from the upstream subscription rather than leaving it running unread.
	if response.Close != nil {
		defer response.Close()
	}

	if len(response.Metadata) > 0 {
		if err := stream.SetHeader(lowercaseMetadata(response.Metadata)); err != nil {
			utils.FormatError("Got error when setting stream header", err, utils.LogAttr("headers", utils.RedactHeaders(response.Metadata)))
		}
	}

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			// Client cancelled or the connection dropped. Report it as the client's
			// own status (Canceled / DeadlineExceeded) rather than a server fault.
			return status.FromContextError(ctx.Err()).Err()
		case msg, open := <-response.Replies:
			if !open {
				if response.Err != nil {
					return response.Err()
				}
				return nil
			}
			if err := stream.SendMsg(msg); err != nil {
				return err
			}
		}
	}
}

// lowercaseMetadata normalizes header keys, which gRPC requires to be lowercase.
func lowercaseMetadata(md metadata.MD) metadata.MD {
	lowercased := metadata.New(map[string]string{})
	for k, v := range md {
		lowercased[strings.ToLower(k)] = v
	}
	return lowercased
}

type RawBytesCodec struct{}

func (RawBytesCodec) Marshal(v interface{}) ([]byte, error) {
	bytes, ok := v.([]byte)
	if !ok {
		return nil, utils.FormatError("cannot encode type", nil, utils.Attribute{Key: "v", Value: v})
	}
	return bytes, nil
}

func (RawBytesCodec) Unmarshal(data []byte, v interface{}) error {
	bufferPtr, ok := v.(*[]byte)
	if !ok {
		return utils.FormatDebug("cannot decode into type", utils.LogAttr("v", v), utils.LogAttr("data", data))
	}
	*bufferPtr = data
	return nil
}

func (RawBytesCodec) Name() string {
	return "lava/grpc-proxy-codec"
}

func (RawBytesCodec) String() string {
	return RawBytesCodec{}.Name()
}

// isHealthRequest reports whether path is a call to the gRPC health service.
func isHealthRequest(path string) bool {
	return strings.HasPrefix(path, "/"+grpc_health_v1.Health_ServiceDesc.ServiceName+"/")
}

// isReflectionRequest checks if the request path is for gRPC reflection service
func isReflectionRequest(path string) bool {
	// gRPC reflection v1alpha (most common)
	if strings.HasPrefix(path, "/grpc.reflection.v1alpha.ServerReflection/") {
		return true
	}
	// gRPC reflection v1 (newer version)
	if strings.HasPrefix(path, "/grpc.reflection.v1.ServerReflection/") {
		return true
	}
	return false
}
