package chainproxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// MAG-2333 regression cover.
//
// grpc.DialContext takes host:port. createConnection stripped the grpc:// / grpcs://
// prefix locally, but increaseNumberOfClients — the on-demand pool refill GetRpc spawns
// — dialed connector.nodeUrl.Url raw. A scheme-prefixed endpoint could therefore open
// its first connection and never grow the pool again: GetRpc respawned a refill every
// 50ms, each dial failed on the unresolvable target, and the caller blocked until its
// context expired.
//
// A one-connection pool makes that fatal rather than merely slow, because the single
// client is retained for reflection and every relay then waits on a refill that cannot
// land. That is the shape `smartrouter health` uses (it builds a router with nConns=1),
// which is why a healthy grpcs:// endpoint reported ok:false while the same host with
// no scheme prefix reported ok:true.

func TestGrpcDialAddress(t *testing.T) {
	for _, tc := range []struct {
		name      string
		url       string
		wantAddr  string
		wantIsTLS bool
	}{
		{name: "grpcs scheme is stripped and implies TLS", url: "grpcs://akash.example.com:443", wantAddr: "akash.example.com:443", wantIsTLS: true},
		{name: "grpc scheme is stripped and does not imply TLS", url: "grpc://akash.example.com:9090", wantAddr: "akash.example.com:9090", wantIsTLS: false},
		{name: "bare host:port is untouched", url: "akash.example.com:443", wantAddr: "akash.example.com:443", wantIsTLS: false},
		{name: "host containing grpc in its name is not mangled", url: "akash-grpc.example.com:443", wantAddr: "akash-grpc.example.com:443", wantIsTLS: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, isTLS := grpcDialAddress(tc.url)
			require.Equal(t, tc.wantAddr, addr)
			require.Equal(t, tc.wantIsTLS, isTLS)
		})
	}
}

// exhaustPoolThenRefill drives the exact sequence that broke: build a one-connection
// pool, borrow its only client so the free list is empty, then ask for another and
// require that the async refill actually produces one. The second GetRpc is what dies
// with DeadlineExceeded when the refill dials a scheme-prefixed target.
func exhaustPoolThenRefill(t *testing.T, nodeUrl common.NodeUrl) {
	t.Helper()

	lifetimeCtx, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()

	dialCtx, cancelDial := context.WithTimeout(lifetimeCtx, 20*time.Second)
	defer cancelDial()

	connector, err := NewGRPCConnector(lifetimeCtx, dialCtx, 1, nodeUrl)
	require.NoError(t, err, "the initial dial normalizes the scheme and must succeed")
	defer connector.Close()

	first, err := connector.GetRpc(dialCtx, true)
	require.NoError(t, err)
	require.NotNil(t, first, "the pool starts with exactly one client")

	// The pool is now empty. Bound this well under the 20s dial ctx so a failure
	// reports as "refill never landed" rather than as the whole test timing out.
	refillCtx, cancelRefill := context.WithTimeout(lifetimeCtx, 10*time.Second)
	defer cancelRefill()

	second, err := connector.GetRpc(refillCtx, true)

	// Hand back every borrowed client BEFORE asserting. require.NoError aborts via
	// FailNow, which runs the deferred Close — and Close spins while usedClients > 0.
	// Asserting while still holding `first` would hang teardown instead of reporting
	// the failure, which is exactly what happens on the unfixed code.
	if second != nil {
		connector.ReturnRpc(second)
	}
	connector.ReturnRpc(first)

	require.NoError(t, err, "GetRpc blocked until its deadline: the async refill never produced a client")
	require.NotNil(t, second)
}

func TestGRPCConnectorRefillsPoolForSchemePrefixedURL(t *testing.T) {
	addr, _ := startRecordingGRPCServer(t)
	exhaustPoolThenRefill(t, common.NodeUrl{Url: "grpc://" + addr})
}

func TestGRPCConnectorRefillsPoolForTLSSchemePrefixedURL(t *testing.T) {
	addr, caCertPath := startTLSReflectionServer(t)
	exhaustPoolThenRefill(t, common.NodeUrl{
		Url: "grpcs://" + addr,
		// The scheme alone is what turns TLS on (createConnection sets UseTLS from it);
		// CaCert is only here so the self-signed test cert is accepted.
		AuthConfig: common.AuthConfig{CaCert: caCertPath},
	})
}

// startTLSReflectionServer starts a gRPC server behind a self-signed cert, with
// reflection registered so it matches the shape of the upstream the router probes.
// It returns the listen address and the path to the CA cert to trust.
func startTLSReflectionServer(t *testing.T) (addr, caCertPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	caCertPath = filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caCertPath, certPEM, 0o600))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	})))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	reflection.Register(srv)
	go srv.Serve(lis)
	t.Cleanup(func() {
		srv.Stop()
		lis.Close()
	})

	return lis.Addr().String(), caCertPath
}
