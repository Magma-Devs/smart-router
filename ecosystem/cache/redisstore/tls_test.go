package redisstore

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

// The context deadline must bound the TLS HANDSHAKE, not only the TCP dial.
// go-redis's stock dialer takes the ctx-less tls.DialWithDialer branch for
// TLS, under which a black-holed endpoint (accepts, never handshakes) keeps
// the dial attempt alive for the full dial-timeout — 5s by go-redis default.
//
// The layer matters: the CALLER of a command was already bounded (go-redis's
// ctx machinery returns deadline-exceeded to it on time either way — verified
// empirically), but the pool's dialConn runs the dialer synchronously, so the
// ATTEMPT itself kept burning its full timeout underneath: the pool slot stays
// held, retries serialize behind it, and the 10s-cadence health probe can
// spend seconds per tick. That is why this test drives baseDialer directly —
// the store-level latency does not discriminate, the dialer's does.
// DialTimeout is set LONG on purpose so only the context's 150ms deadline can
// be what ends the attempt.
func TestTLSHandshakeBoundedByCallerContext(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			// Swallow the ClientHello, never answer: a handshake that stalls.
			go func(c net.Conn) { _, _ = io.Copy(io.Discard, c) }(conn)
		}
	}()

	dial := baseDialer(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = dial(ctx, "tcp", lis.Addr().String())
	elapsed := time.Since(start)
	require.Error(t, err, "a stalled handshake must fail, not hang")
	require.Less(t, elapsed, 2*time.Second,
		"the context's 150ms deadline must bound the handshake — the 5s DialTimeout must not be the effective limit")
}

// testPKI is a throwaway CA with a server and a client leaf, written as PEM
// files so the file-based TLSConfig surface is exercised end to end.
type testPKI struct {
	caFile     string
	serverTLS  tls.Certificate
	clientCert string
	clientKey  string
	caPool     *x509.CertPool
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	return newTestPKIIn(t, t.TempDir())
}

// newTestPKIIn writes the PKI into a caller-chosen directory — the dockerized
// TLS test needs a docker-mountable path (t.TempDir on macOS often is not)
// and container-readable modes.
func newTestPKIIn(t *testing.T, dir string) testPKI {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "resp-cache-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o644)) // throwaway test PKI; container-readable for the dockerized TLS lane
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	leaf := func(cn string, extUsage x509.ExtKeyUsage, certPath, keyPath string) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{extUsage},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		require.NoError(t, err)
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyDER, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		require.NoError(t, os.WriteFile(certPath, certPEM, 0o644)) // throwaway test PKI; container-readable for the dockerized TLS lane
		require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o644))   // throwaway test PKI; container-readable for the dockerized TLS lane
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		return pair
	}

	serverCertPath := filepath.Join(dir, "server.pem")
	serverKeyPath := filepath.Join(dir, "server.key")
	serverPair := leaf("resp-cache-test-server", x509.ExtKeyUsageServerAuth, serverCertPath, serverKeyPath)

	clientCertPath := filepath.Join(dir, "client.pem")
	clientKeyPath := filepath.Join(dir, "client.key")
	leaf("resp-cache-test-client", x509.ExtKeyUsageClientAuth, clientCertPath, clientKeyPath)

	return testPKI{
		caFile:     caFile,
		serverTLS:  serverPair,
		clientCert: clientCertPath,
		clientKey:  clientKeyPath,
		caPool:     caPool,
	}
}

func pingWithTLS(t *testing.T, addr string, tlsCfg TLSConfig) error {
	t.Helper()
	store, err := New(Config{
		Addresses:   []string{addr},
		TLS:         tlsCfg,
		DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
	})
	require.NoError(t, err, "construction must succeed; only the handshake may fail")
	t.Cleanup(func() { _ = store.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return store.Ping(ctx)
}

func TestTLSAcceptance(t *testing.T) {
	pki := newTestPKI(t)
	mr, err := miniredis.RunTLS(&tls.Config{Certificates: []tls.Certificate{pki.serverTLS}})
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	t.Run("trusted CA and server name succeeds", func(t *testing.T) {
		require.NoError(t, pingWithTLS(t, mr.Addr(), TLSConfig{
			Enabled:    true,
			CAFile:     pki.caFile,
			ServerName: "localhost",
		}))
	})

	t.Run("missing CA fails verification", func(t *testing.T) {
		require.Error(t, pingWithTLS(t, mr.Addr(), TLSConfig{
			Enabled:    true,
			ServerName: "localhost",
		}), "the system pool must not trust the test CA")
	})

	t.Run("wrong CA fails verification", func(t *testing.T) {
		otherPKI := newTestPKI(t)
		require.Error(t, pingWithTLS(t, mr.Addr(), TLSConfig{
			Enabled:    true,
			CAFile:     otherPKI.caFile,
			ServerName: "localhost",
		}))
	})

	t.Run("insecure-skip-verify connects without a CA", func(t *testing.T) {
		require.NoError(t, pingWithTLS(t, mr.Addr(), TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: true,
		}))
	})
}

func TestMutualTLSAcceptance(t *testing.T) {
	pki := newTestPKI(t)
	mr, err := miniredis.RunTLS(&tls.Config{
		Certificates: []tls.Certificate{pki.serverTLS},
		ClientCAs:    pki.caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	t.Run("client certificate round-trips", func(t *testing.T) {
		require.NoError(t, pingWithTLS(t, mr.Addr(), TLSConfig{
			Enabled:    true,
			CAFile:     pki.caFile,
			ServerName: "localhost",
			CertFile:   pki.clientCert,
			KeyFile:    pki.clientKey,
		}))
	})

	t.Run("missing client certificate is rejected", func(t *testing.T) {
		require.Error(t, pingWithTLS(t, mr.Addr(), TLSConfig{
			Enabled:    true,
			CAFile:     pki.caFile,
			ServerName: "localhost",
		}))
	})
}
