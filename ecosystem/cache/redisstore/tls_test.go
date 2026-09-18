package redisstore

import (
	"bytes"
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
	"sync"
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

// wireCaptureListener accepts every connection and keeps the bytes each one
// sends, never answering — the recording endpoint that stood in for the store
// when the evidence on MAG-3683 was captured. received returns a copy of
// everything captured so far.
func wireCaptureListener(t *testing.T) (net.Listener, func() []byte) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	var mu sync.Mutex
	var captured bytes.Buffer
	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				chunk := make([]byte, 4096)
				for {
					n, readErr := c.Read(chunk)
					if n > 0 {
						mu.Lock()
						captured.Write(chunk[:n])
						mu.Unlock()
					}
					if readErr != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return lis, func() []byte {
		mu.Lock()
		defer mu.Unlock()
		return append([]byte(nil), captured.Bytes()...)
	}
}

// MAG-3683: a tls block with every file path written and no tls.enabled. The
// files are read only when the switch is on, so before this fix the block was
// inert: the router started, reported TLS off in one word among six fields,
// opened a plaintext connection, and its first write carried the username and
// password readable.
//
// Three directions, each keeping the previous one honest:
//
//  1. no tls block at all, with a password — the credential IS on the wire in
//     the clear. That configuration is deliberate and stays allowed; here it
//     proves the instrument sees a plaintext credential when one is sent.
//  2. the ticket's block without the switch — refused before any dial, naming
//     the missing key.
//  3. the same block with the switch on — the listener sees a TLS handshake
//     and never the credential (the control without which 2 passes on a store
//     that sends nothing), and a missing CA file aborts construction naming
//     the file (the ticket's second control: the files ARE read once the
//     switch is on).
func TestTLSBlockWithoutEnabledNeverSendsPlaintext(t *testing.T) {
	const username, password = "cacheuser", "placeholder-cache-credential"
	newStore := func(t *testing.T, addr string, tlsCfg TLSConfig) (*Store, error) {
		t.Helper()
		store, err := New(Config{
			Addresses:   []string{addr},
			Username:    username,
			Password:    password,
			TLS:         tlsCfg,
			DialTimeout: time.Second,
			ReadTimeout: time.Second,
		})
		if store != nil {
			t.Cleanup(func() { _ = store.Close() })
		}
		return store, err
	}
	pingOnce := func(t *testing.T, store *Store) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		require.Error(t, store.Ping(ctx), "the listener never answers, so the operation must fail")
	}
	missing := filepath.Join(t.TempDir(), "does-not-exist", "ca.pem")

	t.Run("no tls block: the credential crosses in the clear (instrument check)", func(t *testing.T) {
		lis, received := wireCaptureListener(t)
		store, err := newStore(t, lis.Addr().String(), TLSConfig{})
		require.NoError(t, err)
		pingOnce(t, store)
		require.Eventually(t, func() bool { return bytes.Contains(received(), []byte(password)) },
			2*time.Second, 20*time.Millisecond,
			"without TLS the password is readable on the wire; the listener must see it, or the assertions below prove nothing")
	})

	t.Run("tls block without the switch is refused before any dial", func(t *testing.T) {
		lis, received := wireCaptureListener(t)
		_, err := newStore(t, lis.Addr().String(), TLSConfig{CAFile: missing, CertFile: missing, KeyFile: missing})
		require.ErrorContains(t, err, "tls.enabled",
			"a tls block without its switch must be refused, not started in plaintext")
		require.Empty(t, received(), "nothing may reach the wire")
	})

	t.Run("control: with the switch on the listener sees a handshake, never the credential", func(t *testing.T) {
		lis, received := wireCaptureListener(t)
		pki := newTestPKI(t)
		store, err := newStore(t, lis.Addr().String(), TLSConfig{Enabled: true, CAFile: pki.caFile, ServerName: "localhost"})
		require.NoError(t, err)
		pingOnce(t, store)
		require.Eventually(t, func() bool { return len(received()) >= 3 }, 2*time.Second, 20*time.Millisecond,
			"the client must have sent its ClientHello")
		got := received()
		require.Equal(t, byte(0x16), got[0], "first byte is a TLS handshake record")
		require.Equal(t, byte(0x03), got[1], "TLS record-layer major version")
		require.NotContains(t, string(got), password, "the credential must never appear in the clear")
		require.NotContains(t, string(got), username)
	})

	t.Run("control: with the switch on a missing CA file aborts construction naming the file", func(t *testing.T) {
		lis, received := wireCaptureListener(t)
		_, err := newStore(t, lis.Addr().String(), TLSConfig{Enabled: true, CAFile: missing})
		require.ErrorContains(t, err, missing)
		require.Empty(t, received())
	})
}
