package performance

// An authentication failure must be reported AS an authentication failure —
// both the behaviour and its risk mitigation. Before this,
// the health probe reduced Ping to a boolean and every fault logged
// "unreachable" — sending an operator to check networking when the real cause
// was a credential.
//
// These tests pin the classification and, just as importantly, that the log
// detail never carries a secret.

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	authTestUser = "cacheuser"
	// A deliberately placeholder-shaped value: it still has to be distinctive
	// for the NotContains assertions below to mean anything, but it must not
	// read as a real credential to a secret scanner.
	authTestPassword = "placeholder-not-a-real-credential"
)

// pingErr returns the error the health probe would observe for a given client.
func pingErr(t *testing.T, client *redis.Client) error {
	t.Helper()
	store, err := redisstore.NewWithClient(client, "sr")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return store.Ping(ctx)
}

// A wrong password against an auth-required backend must classify as an
// authentication failure, not a connection failure.
func TestProbeErrorClassifiesAuthFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireUserAuth(authTestUser, authTestPassword)

	client := redis.NewClient(&redis.Options{
		Addr:     mr.Addr(),
		Username: authTestUser,
		Password: "definitely-the-wrong-password",
	})
	t.Cleanup(func() { _ = client.Close() })

	err := pingErr(t, client)
	require.Error(t, err, "an authenticated backend must reject a bad credential")
	require.True(t, redis.IsAuthError(err), "go-redis must recognise this as an auth error: %v", err)
	require.Equal(t, probeFailureAuth, classifyProbeError(err),
		"a rejected credential must be classified as an authentication failure")
}

// The logged detail must never contain the credential, the username, or the
// server's credential-naming reply.
func TestProbeErrorDetailNeverLeaksCredentials(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireUserAuth(authTestUser, authTestPassword)

	client := redis.NewClient(&redis.Options{
		Addr:     mr.Addr(),
		Username: authTestUser,
		Password: authTestPassword + "-wrong",
	})
	t.Cleanup(func() { _ = client.Close() })

	detail := safeProbeDetail(pingErr(t, client))

	require.NotContains(t, detail, authTestPassword, "the password must never reach the log")
	require.NotContains(t, detail, authTestUser, "the username must never reach the log")
	require.NotContains(t, strings.ToUpper(detail), "WRONGPASS",
		"the server's credential-naming reply must not be echoed verbatim")
	require.Equal(t, "backend rejected the configured credentials", detail,
		"auth failures collapse to a fixed, secret-free phrase")
}

// A genuine connectivity fault must NOT be reported as an auth failure —
// the classification has to discriminate, not default to one answer.
func TestProbeErrorClassifiesConnectionFailure(t *testing.T) {
	// Bind then immediately close, so the port is almost certainly refused.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	client := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: time.Second})
	t.Cleanup(func() { _ = client.Close() })

	probeError := pingErr(t, client)
	require.Error(t, probeError, "a closed port must fail the probe")
	require.Equal(t, probeFailureConnection, classifyProbeError(probeError),
		"a dial failure must classify as a connection failure, not authentication")

	// Non-auth detail is a network fault and is safe to surface verbatim.
	require.NotEmpty(t, safeProbeDetail(probeError))
	require.NotEqual(t, "backend rejected the configured credentials", safeProbeDetail(probeError))
}

// A healthy backend produces no error and therefore no failure classification
// beyond the default; guards against the classifier mislabelling success.
func TestProbeErrorNilIsNotAuthFailure(t *testing.T) {
	require.Equal(t, probeFailureConnection, classifyProbeError(nil))
	require.Equal(t, "no error reported", safeProbeDetail(nil))
}
