package chainproxy

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
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// cacert-pem names the roots the server is verified against: the connector dials a
// self-signed upstream with that certificate as the CA, and refuses the same upstream
// with an unrelated CA. Verification used to be switched off right after the CA pool was
// loaded, so the unrelated CA connected too (MAG-4278 #13).
func TestGRPCConnector_VerifiesTheServerAgainstTheConfiguredCA(t *testing.T) {
	serverTLS, caPath, _ := testcert.SelfSigned(t)
	_, otherCAPath, _ := testcert.SelfSigned(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	url := "grpcs://" + listener.Addr().String()

	t.Run("the configured CA verifies the server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		connector, err := NewGRPCConnector(ctx, ctx, 1, common.NodeUrl{Url: url, AuthConfig: common.AuthConfig{CaCert: caPath}})
		require.NoError(t, err)
		connector.Close()
	})
	t.Run("an unrelated CA refuses the server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := NewGRPCConnector(ctx, ctx, 1, common.NodeUrl{Url: url, AuthConfig: common.AuthConfig{CaCert: otherCAPath}})
		require.Error(t, err, "a CA that did not sign the server must not verify it")
	})
	t.Run("allow-insecure skips verification", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		connector, err := NewGRPCConnector(ctx, ctx, 1, common.NodeUrl{Url: url, AuthConfig: common.AuthConfig{AllowInsecure: true}})
		require.NoError(t, err)
		connector.Close()
	})
}

// cert-pem/key-pem is presented beside cacert-pem: an upstream that requires a client
// certificate signed by a CA it trusts accepts a connector configured with all three, the
// mutual-TLS setup a private CA implies. The client certificate used to be loaded only when
// no CA was configured, so this setup presented none and every call was refused.
func TestGRPCConnector_PresentsTheClientCertificateBesideTheConfiguredCA(t *testing.T) {
	serverTLS, caPath, _ := testcert.SelfSigned(t)
	_, clientCertPath, clientKeyPath := testcert.SelfSigned(t)
	clientPEM, err := os.ReadFile(clientCertPath)
	require.NoError(t, err)
	clientCAs := x509.NewCertPool()
	require.True(t, clientCAs.AppendCertsFromPEM(clientPEM))
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	serverTLS.ClientCAs = clientCAs

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connector, err := NewGRPCConnector(ctx, ctx, 1, common.NodeUrl{
		Url:        "grpcs://" + listener.Addr().String(),
		AuthConfig: common.AuthConfig{CaCert: caPath, CertPem: clientCertPath, KeyPem: clientKeyPath},
	})
	require.NoError(t, err)
	defer connector.Close()
	conn, err := connector.GetRpc(ctx, true)
	require.NoError(t, err)
	// Under TLS 1.3 the client completes its handshake before the server has checked the
	// certificate, so only a completed call shows the upstream accepted it.
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	connector.ReturnRpc(conn)
	require.NoError(t, err, "the upstream requires a client certificate signed by a CA it trusts")
}
