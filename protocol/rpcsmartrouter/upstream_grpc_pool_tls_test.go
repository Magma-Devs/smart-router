package rpcsmartrouter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils/testcert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
)

// startTLSHoldingStreamServer is startHoldingStreamServer behind TLS.
func startTLSHoldingStreamServer(t *testing.T, serverTLS *tls.Config) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	testgrpc.RegisterTestServiceServer(server, holdingStreamService{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// The stream pool builds its TLS from the endpoint's auth-config the way the unary
// connector does, so cacert-pem is honoured on subscriptions too. It used to read
// allow-insecure alone: a CA configured for an endpoint verified its unary calls and was
// never loaded for its streams, so the same endpoint was reachable for calls and failed
// TLS for streams (MAG-4278 #13).
func TestUpstreamGRPCPool_HonoursTheEndpointsCACertificate(t *testing.T) {
	serverTLS, caPath, _ := testcert.SelfSigned(t)
	addr := startTLSHoldingStreamServer(t, serverTLS)
	config := idleOnlyConfig(0)
	config.ConnectionTimeout = 2 * time.Second

	openWith := func(t *testing.T, auth common.AuthConfig) error {
		t.Helper()
		pool := NewUpstreamGRPCPoolWithConfig(&common.NodeUrl{Url: "grpcs://" + addr, AuthConfig: auth}, config)
		t.Cleanup(func() { _ = pool.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := openPooledStream(ctx, pool)
		return err
	}

	require.NoError(t, openWith(t, common.AuthConfig{CaCert: caPath}), "the configured CA verifies the self-signed upstream")
	require.Error(t, openWith(t, common.AuthConfig{}), "without the CA the self-signed upstream is refused")
	require.NoError(t, openWith(t, common.AuthConfig{AllowInsecure: true}), "allow-insecure still skips verification")
}

// The stream pool presents cert-pem/key-pem beside cacert-pem too: the same auth-config
// that verifies the upstream against its CA opens a stream to an upstream that requires a
// client certificate. The pool used to present none, and the shared TLS configuration
// still dropped the certificate whenever a CA was configured.
func TestUpstreamGRPCPool_PresentsTheClientCertificateBesideTheConfiguredCA(t *testing.T) {
	serverTLS, caPath, _ := testcert.SelfSigned(t)
	_, clientCertPath, clientKeyPath := testcert.SelfSigned(t)
	clientPEM, err := os.ReadFile(clientCertPath)
	require.NoError(t, err)
	clientCAs := x509.NewCertPool()
	require.True(t, clientCAs.AppendCertsFromPEM(clientPEM))
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	serverTLS.ClientCAs = clientCAs
	addr := startTLSHoldingStreamServer(t, serverTLS)

	config := idleOnlyConfig(0)
	config.ConnectionTimeout = 2 * time.Second
	pool := NewUpstreamGRPCPoolWithConfig(&common.NodeUrl{
		Url:        "grpcs://" + addr,
		AuthConfig: common.AuthConfig{CaCert: caPath, CertPem: clientCertPath, KeyPem: clientKeyPath},
	}, config)
	t.Cleanup(func() { _ = pool.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = openPooledStream(ctx, pool)
	require.NoError(t, err, "the upstream requires a client certificate signed by a CA it trusts")
}
