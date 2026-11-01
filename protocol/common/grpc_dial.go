package common

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCMaxCallRecvMsgSize caps a gRPC message on every path the router dials or serves:
// what the unary connector, the stream pool and the cache client accept from the far
// side, and what the gRPC listener and the cache server accept from a client. One
// constant, because the unary connector and the stream pool dial the same upstreams, and
// a divergence between them would mean the same endpoint accepted a response on one path
// and rejected it on the other. 512 MB, for large debug responses.
const GRPCMaxCallRecvMsgSize = 1024 * 1024 * 512

// GRPCStatusCodeOnFailedMessages is the status code a gRPC relay reports when the node's
// error carries no gRPC status of its own: outside the range gRPC defines (0-16), so a
// reader cannot mistake it for a real code.
const GRPCStatusCodeOnFailedMessages = 32

// TLSConfig is the client TLS configuration this auth-config asks for, built the same
// way for every gRPC dialer in the router (the unary connector, the stream pool and the
// cache client), so an endpoint's cacert-pem, cert-pem/key-pem and allow-insecure mean
// the same thing on a unary call as on a subscription stream (MAG-4278: the stream pool
// used to read allow-insecure only, so a CA configured for an endpoint verified its
// unary calls and was never loaded for its streams).
//
// cacert-pem names the roots the server is verified against, instead of the system
// roots; it is what it says, a trust anchor, and verification stays on (it used to be
// switched off right after the pool was loaded, which left the setting with no effect
// beyond the log line). cert-pem/key-pem is the client certificate to present, with or
// without cacert-pem: the two settings fill independent halves of the handshake, and
// together they are the mutual-TLS setup a private CA implies (the client certificate
// used to be loaded only when no CA was configured, so that setup silently presented
// none). allow-insecure disables server verification altogether, which is what a
// self-signed development endpoint needs.
//
// A cacert-pem that cannot be loaded (the file is unreadable, or holds no PEM
// certificate) fails closed: it is logged and nothing is trusted, so every server fails
// verification until the file is fixed. Falling back to the system roots would turn a
// mistyped path or a mount that never happened into trust in every public CA, for the
// one setting whose purpose is to narrow trust to one CA; the RESP cache makes the same
// call for its CA file (MAG-3683). allow-insecure still disables verification.
func (ac *AuthConfig) TLSConfig() *tls.Config {
	tlsConf := &tls.Config{}
	if ac == nil {
		return tlsConf
	}
	if cacert := ac.GetCaCertificateParams(); cacert != "" {
		utils.LavaFormatDebug("Loading ca certificate from local path", utils.LogAttr("cacert", cacert))
		// An empty pool, not nil: nil means the system roots, and a CA that was configured
		// but could not be loaded must not widen trust to them.
		caCertPool := x509.NewCertPool()
		caCert, err := os.ReadFile(cacert)
		switch {
		case err != nil:
			utils.LavaFormatError("Failed loading CA certificate, no server will verify until it is fixed", err, utils.LogAttr("cacert", cacert))
		case !caCertPool.AppendCertsFromPEM(caCert):
			utils.LavaFormatError("CA certificate file holds no PEM certificate, no server will verify until it is fixed", nil, utils.LogAttr("cacert", cacert))
		}
		tlsConf.RootCAs = caCertPool
	}
	if keyPem, certPem := ac.GetLoadingCertificateParams(); keyPem != "" && certPem != "" {
		utils.LavaFormatDebug("Loading certificate from local path", utils.LogAttr("certPem", certPem), utils.LogAttr("keyPem", keyPem))
		cert, err := tls.LoadX509KeyPair(certPem, keyPem)
		if err != nil {
			utils.LavaFormatError("Failed setting up tls certificate from local path, continuing with dynamic certificates", err)
		} else {
			tlsConf.Certificates = []tls.Certificate{cert}
		}
	}
	if ac.AllowInsecure {
		tlsConf.InsecureSkipVerify = true
	}
	return tlsConf
}

// GRPCTransportCredentials is the transport a dial uses: TLS from TLSConfig when the
// endpoint is secure (grpcs://, or use-tls), the plaintext transport otherwise.
func (ac *AuthConfig) GRPCTransportCredentials(secure bool) grpc.DialOption {
	if !secure {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(ac.TLSConfig()))
}

// GRPCDialOptions assembles the options every gRPC dial to this endpoint shares: the
// transport the caller decided on, the receive-size cap, and (MAG-2218) the endpoint's
// configured auth-headers, followed by whatever the caller's channel needs on top. Every
// dialer goes through it, so the auth attachment cannot be present on one path and
// missing on another, which is the class of bug MAG-2218 was, and the cap cannot differ
// between the unary and the streaming path to the same node.
//
// What stays with the caller is what genuinely differs between channels: the unary
// connector and the cache client dial blocking (grpc.WithBlock, they want a fast failure
// for an unreachable endpoint), the stream pool dials lazily and adds the MAG-3887 idle
// timeout and keepalive, which protect long-lived streams from a middlebox that forgot
// the flow. Whether unary channels want the same keepalive is an open decision, not an
// omission: a unary channel that sits idle is closed and redialled by the connector's
// own retry, and a grpc-go server GOAWAYs a client that pings too often.
func (nodeUrl *NodeUrl) GRPCDialOptions(transport grpc.DialOption, extra ...grpc.DialOption) []grpc.DialOption {
	authOpts := nodeUrl.GrpcAuthDialOptions()
	opts := make([]grpc.DialOption, 0, 2+len(authOpts)+len(extra))
	opts = append(opts, transport, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(GRPCMaxCallRecvMsgSize)))
	opts = append(opts, authOpts...)
	return append(opts, extra...)
}
