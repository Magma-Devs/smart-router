package grpcproxy

import (
	"context"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const MaxCallRecvMsgSize = 1024 * 1024 * 512 // 512MB for large debug responses

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
	// the upstream stream ends; the proxy then closes the client stream with OK.
	Replies <-chan []byte

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

func NewGRPCProxy(cb ProxyCallBack, healthCheckPath string, cmdFlags common.ConsumerCmdFlags, healthReporter HealthReporter) (*grpc.Server, *http.Server, error) {
	return NewGRPCProxyWithReflection(cb, healthCheckPath, cmdFlags, healthReporter, nil, nil)
}

// NewGRPCProxyWithReflection creates a gRPC proxy with optional reflection support.
// If reflectionCallback is provided, a separate gRPC server is created for reflection
// that uses standard protobuf codec (not RawBytesCodec), allowing proper serialization.
// This enables tools like grpcurl to work with the smart router.
//
// streamCallback is optional too; when non-nil the proxy offers every request to it
// first, so server-streaming methods are served as streams instead of being forced
// through the single-message unary path.
func NewGRPCProxyWithReflection(cb ProxyCallBack, healthCheckPath string, cmdFlags common.ConsumerCmdFlags, healthReporter HealthReporter, reflectionCallback ReflectionProxyCallback, streamCallback StreamProxyCallBack) (*grpc.Server, *http.Server, error) {
	serverReceiveMaxMessageSize := grpc.MaxRecvMsgSize(MaxCallRecvMsgSize) // setting receive size to 32mb instead of 4mb default
	s := grpc.NewServer(grpc.UnknownServiceHandler(makeProxyFunc(cb, streamCallback)), grpc.ForceServerCodec(RawBytesCodec{}), serverReceiveMaxMessageSize)
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())

	wrappedServer := grpcweb.WrapServer(s)

	// Create a separate gRPC server for reflection with standard protobuf codec
	// This is needed because the main proxy server uses RawBytesCodec which breaks
	// the reflection service's protobuf message serialization
	var wrappedReflectionServer *grpcweb.WrappedGrpcServer
	if reflectionCallback != nil {
		reflectionGrpcServer := grpc.NewServer(serverReceiveMaxMessageSize)
		RegisterReflectionProxy(reflectionGrpcServer, reflectionCallback)
		wrappedReflectionServer = grpcweb.WrapServer(reflectionGrpcServer)
	}

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

		// Route reflection requests to the dedicated reflection server
		if wrappedReflectionServer != nil && isReflectionRequest(req.URL.Path) {
			wrappedReflectionServer.ServeHTTP(resp, req)
			return
		}

		wrappedServer.ServeHTTP(resp, req)
	}

	httpServer := &http.Server{
		Handler: h2c.NewHandler(http.HandlerFunc(handler), &http2.Server{}),
	}

	return s, httpServer, nil
}

func makeProxyFunc(callBack ProxyCallBack, streamCallBack StreamProxyCallBack) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		// currently the callback function does not account for headers.
		methodName, ok := grpc.MethodFromServerStream(stream)
		if !ok {
			return status.Error(codes.Unavailable, "unable to get method name")
		}
		var reqBytes []byte
		err := stream.RecvMsg(&reqBytes)
		if err != nil {
			return err
		}
		method := methodName[1:] // strip first '/' of the method name

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

		respBytes, md, err := callBack(stream.Context(), method, reqBytes)

		md = lowercaseMetadata(md)

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
			utils.LavaFormatError("Got error when setting header", err, utils.LogAttr("headers", md))
		}
		return stream.SendMsg(respBytes)
	}
}

// serveServerStream holds the client stream open and forwards upstream messages onto
// it until the upstream ends or the client goes away. This is the listener half the
// Direct RPC gRPC path was missing: the subscription manager could already open an
// upstream stream and fan it out to per-client channels, but nothing on the serving
// side kept a client connection alive to read one (MAG-2643).
//
// Returns nil on upstream end-of-stream, which closes the client stream with OK.
func serveServerStream(stream grpc.ServerStream, response *StreamResponse) error {
	// Runs on every exit path, so a client that disconnects mid-stream is dropped
	// from the upstream subscription rather than leaving it running unread.
	if response.Close != nil {
		defer response.Close()
	}

	if len(response.Metadata) > 0 {
		if err := stream.SetHeader(lowercaseMetadata(response.Metadata)); err != nil {
			utils.LavaFormatError("Got error when setting stream header", err, utils.LogAttr("headers", response.Metadata))
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

func (RawBytesCodec) Marshal(v any) ([]byte, error) {
	bytes, ok := v.([]byte)
	if !ok {
		return nil, utils.LavaFormatError("cannot encode type", nil, utils.Attribute{Key: "v", Value: v})
	}
	return bytes, nil
}

func (RawBytesCodec) Unmarshal(data []byte, v any) error {
	bufferPtr, ok := v.(*[]byte)
	if !ok {
		return utils.LavaFormatDebug("cannot decode into type", utils.LogAttr("v", v), utils.LogAttr("data", data))
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
