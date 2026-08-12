package common

import (
	"context"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
)

// MAG-2218: auth-headers were honoured by every transport EXCEPT gRPC — HTTP sets them
// in DoHTTPRequest, WebSocket in ensureClient, REST/TendermintRPC on the outgoing request,
// and the JSON-RPC connector via rpcClient.SetHeader. gRPC read them nowhere, so an
// authenticated upstream (Canton's Ledger API being the case that surfaced it) answered
// Unauthenticated for every call, including the boot-time spec verification.
//
// The fix attaches them at DIAL time rather than at each call site: both gRPC stacks
// (the chainlib GrpcChainProxy used by boot verification, and the lavasession
// GRPCDirectRPCConnection used by relays and per-endpoint polls) construct their
// connections through chainproxy.GRPCConnector, and the subscription manager's pool is
// the only other dialler. Attaching there covers unary calls, server reflection and
// streams at once, so the paths cannot drift apart later.

// grpcAuthCredentials attaches statically-configured auth headers to every outgoing gRPC
// call. It implements google.golang.org/grpc/credentials.PerRPCCredentials.
type grpcAuthCredentials struct {
	headers map[string]string
}

// GetRequestMetadata returns the configured headers for every call. The context and the
// call URI are ignored — these headers are static per endpoint, not per request.
func (c grpcAuthCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return c.headers, nil
}

// RequireTransportSecurity reports false so the credentials can be used on plaintext
// (grpc://) upstreams. Returning true would make grpc-go refuse the dial outright
// whenever the transport is insecure.NewCredentials(), which is exactly the local-node
// and private-network case operators run against. See TokenOverInsecureWarning for the
// operator-facing warning that accompanies this choice.
func (c grpcAuthCredentials) RequireTransportSecurity() bool { return false }

// isLegalGrpcHeaderName reports whether name is usable as a gRPC metadata key.
// grpc-go lowercases keys on the way out, so the check runs against the lowered form.
// An illegal name is rejected by the HTTP/2 transport at call time rather than at config
// parse, which surfaces as an opaque per-relay failure — so it is screened here instead.
func isLegalGrpcHeaderName(name string) bool {
	if name == "" {
		return false
	}
	// "-bin" keys carry binary metadata and require base64-encoded values; auth headers
	// configured as YAML strings are not that, so reject rather than corrupt them.
	if strings.HasSuffix(name, "-bin") {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

// GrpcAuthDialOptions returns the dial options that attach this endpoint's configured
// auth-headers to every outgoing gRPC call. It returns nil when no auth headers are
// configured, so callers can append unconditionally.
//
// Header names are lowercased (grpc-go does this anyway) and screened; illegal names are
// dropped with an error rather than being allowed to fail opaquely at call time.
func (url *NodeUrl) GrpcAuthDialOptions() []grpc.DialOption {
	if url == nil {
		return nil
	}
	configured := url.GetAuthHeaders()
	if len(configured) == 0 {
		return nil
	}

	headers := make(map[string]string, len(configured))
	for name, value := range configured {
		lowered := strings.ToLower(name)
		if !isLegalGrpcHeaderName(lowered) {
			utils.FormatError("dropping illegal gRPC auth-header name", nil,
				utils.LogAttr("header", name),
				utils.LogAttr("url", url.UrlStr()),
				utils.LogAttr("hint", "gRPC metadata keys allow a-z, 0-9, '-', '_', '.' and must not end in -bin"),
			)
			continue
		}
		headers[lowered] = value
	}
	if len(headers) == 0 {
		return nil
	}

	return []grpc.DialOption{grpc.WithPerRPCCredentials(grpcAuthCredentials{headers: headers})}
}

// TokenOverInsecureWarning emits a one-line warning when auth headers are configured for
// an endpoint whose transport is not secured. The credentials deliberately permit this
// (see RequireTransportSecurity), matching how the HTTP and WebSocket paths already send
// auth headers over plaintext http:// — but a bearer token on the wire in the clear is
// worth telling the operator about. Callers pass the transport decision they actually
// made, since the scheme alone does not determine it.
func (url *NodeUrl) TokenOverInsecureWarning(transportIsSecure bool) {
	if url == nil || transportIsSecure || len(url.GetAuthHeaders()) == 0 {
		return
	}
	utils.FormatWarning("sending gRPC auth headers over an insecure (plaintext) connection", nil,
		utils.LogAttr("url", url.UrlStr()),
		utils.LogAttr("hint", "use grpcs:// or auth-config.use-tls to protect the credential in transit"),
	)
}
