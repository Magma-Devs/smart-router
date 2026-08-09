package redisstore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/stretchr/testify/require"
)

// Real-server TLS proof: a dockerized Valkey terminating TLS with
// --tls-auth-clients yes (mTLS REQUIRED), our generated PKI mounted in, and
// the store connecting through the file-based TLSConfig surface end to end —
// including the negative case (no client certificate → rejected by the real
// server). Complements the miniredis.RunTLS unit suite.
//
//	RESP_CACHE_TEST_TLS_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestTLSDockerValkey -v -timeout 3m
func TestTLSDockerValkey(t *testing.T) {
	requireDockerLane(t, "RESP_CACHE_TEST_TLS_DOCKER", "the real-server TLS lane")
	const (
		container = "sr-tls-valkey"
		hostPort  = 63794
	)

	// The PKI must live under the repo (docker-shareable on macOS, unlike the
	// default TempDir locations).
	certDir, err := os.MkdirTemp(".", "tls-docker-certs-")
	require.NoError(t, err)
	certDir, err = filepath.Abs(certDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(certDir) })
	pki := newTestPKIIn(t, certDir)

	docker := func(args ...string) (string, error) {
		out, err := exec.Command("docker", args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	_, _ = docker("rm", "-f", container)
	t.Cleanup(func() { _, _ = docker("rm", "-f", container) })

	out, err := docker("run", "--rm", "-d", "--name", container,
		"-p", fmt.Sprintf("127.0.0.1:%d:6379", hostPort),
		"-v", certDir+":/certs:ro",
		"valkey/valkey:7.2",
		"valkey-server", "--port", "0", "--tls-port", "6379",
		"--tls-cert-file", "/certs/server.pem",
		"--tls-key-file", "/certs/server.key",
		"--tls-ca-cert-file", "/certs/ca.pem",
		"--tls-auth-clients", "yes")
	require.NoError(t, err, out)

	addr := fmt.Sprintf("127.0.0.1:%d", hostPort)
	mtls := TLSConfig{
		Enabled:    true,
		CAFile:     pki.caFile,
		CertFile:   pki.clientCert,
		KeyFile:    pki.clientKey,
		ServerName: "localhost",
	}

	store, err := New(Config{Addresses: []string{addr}, TLS: mtls, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	require.Eventually(t, func() bool { return store.Ping(ctx) == nil },
		30*time.Second, 500*time.Millisecond, "mTLS handshake against real Valkey must succeed")

	key := core.HeightKey("ETH1", "tls-docker")
	require.NoError(t, store.SetHeight(ctx, key, 42, time.Minute))
	v, found, err := store.GetHeight(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(42), v, "reads and writes flow over the TLS session")

	// Negative: the real server REQUIRES a client certificate.
	noCert, err := New(Config{
		Addresses:   []string{addr},
		TLS:         TLSConfig{Enabled: true, CAFile: pki.caFile, ServerName: "localhost"},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = noCert.Close() })
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.Error(t, noCert.Ping(pingCtx), "a client without a certificate must be rejected by --tls-auth-clients yes")
}
