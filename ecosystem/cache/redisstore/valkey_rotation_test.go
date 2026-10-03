package redisstore

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// The live-rotation acceptance proof against a REAL Valkey/Redis — miniredis
// implements neither CONFIG, CLIENT, nor ACL, so it cannot express this.
//
//	docker run --rm -p 127.0.0.1:63790:6379 valkey/valkey:7.2
//	RESP_CACHE_TEST_VALKEY_ADDR=127.0.0.1:63790 go test ./ecosystem/cache/redisstore -run LiveRotationAgainstRealValkey -v
//
// Asserts what the client guarantees: rotating between two ACL users through
// the streaming provider keeps operations flowing, and afterwards the
// connection established before the rotation is either re-authenticated IN
// PLACE (the server-side CLIENT LIST shows the SAME connection id running as
// the new user) or replaced by one that runs as the new user. Mere op success
// would prove nothing (a server password change never de-auths existing
// connections, and both ACL users can run the operation), so the proof reads
// the server's view and asks the store's own connection who it is (servedAs).
// A dial counter additionally bounds dials: go-redis may legitimately dial ONE
// extra connection while the original is being re-authed (checkout prefers a
// fresh conn over blocking), but a reconnect storm means the rotation dropped
// connections.
func TestLiveRotationAgainstRealValkey(t *testing.T) {
	addr := os.Getenv("RESP_CACHE_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("set RESP_CACHE_TEST_VALKEY_ADDR to a real Valkey/Redis (docker run --rm -d --name rot-valkey -p 127.0.0.1:63795:6379 valkey/valkey:7.2) to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	// Opting in without a reachable server is a hard failure, not a skip (same
	// rule as requireDockerLane) — but say what is missing, because the bare
	// dial error reads like a product fault rather than a missing prerequisite.
	require.NoError(t, admin.Ping(ctx).Err(),
		"RESP_CACHE_TEST_VALKEY_ADDR=%s is set but nothing is listening there — start the server first:\n"+
			"  docker run --rm -d --name rot-valkey -p 127.0.0.1:63795:6379 valkey/valkey:7.2", addr)

	for _, user := range []string{"rotuser1", "rotuser2"} {
		require.NoError(t, admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+user+"-pw", "~*", "+@all").Err())
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = admin.Do(cleanupCtx, "ACL", "DELUSER", "rotuser1", "rotuser2").Err()
	})

	credFile := writeTempFile(t, "cred", "rotuser1:rotuser1-pw")
	provider := NewStreamingProvider(&FileCredentials{Path: credFile})

	// One connection is established before the rotation (no MinIdleConns), and
	// the pool has room for one more: a connection whose re-authentication
	// stalled (see rotationSettled) keeps its slot until the pool timeout, and
	// with PoolSize 1 that left no room for the replacement, so every operation
	// during the stall dialled one and dropped it again, a dozen dials in six
	// seconds. That is what a pool at its size limit does in production too; the
	// dial bound below is about a pool with capacity, the ordinary case.
	client := redis.NewClient(&redis.Options{
		Addr:                         addr,
		StreamingCredentialsProvider: provider,
		PoolSize:                     2,
	})
	dials := &dialCounter{}
	client.AddHook(dials)
	store, err := NewWithClient(client, "rotparity")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	op := func() error { return store.SetHeight(ctx, core.HeightKey("ETH1", "0xh"), 1, time.Minute) }
	require.NoError(t, op())

	connID, err := client.Do(ctx, "CLIENT", "ID").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), dials.count())

	// Rotate: the file flips to the second ACL user; the provider pushes.
	require.NoError(t, os.WriteFile(credFile, []byte("rotuser2:rotuser2-pw"), 0o600))
	provider.Refresh()

	for i := 0; i < 10; i++ {
		require.NoError(t, op(), "operations must continue through the rotation")
		time.Sleep(20 * time.Millisecond)
	}

	// Server-side proof, read from CLIENT LIST: the ORIGINAL connection is either
	// alive and now reporting the new user (re-authenticated in place) or gone
	// (closed by the client and replaced), and traffic runs as the new user. See
	// rotationSettled for why both are the guarantee, and rotationSettleTimeout
	// for the bound.
	established := map[string]bool{strconv.FormatInt(connID, 10): true}
	var inPlace, replaced int
	require.Eventually(t, func() bool {
		settled, ip, rp := rotationSettled(ctx, t, admin, established, "rotuser2")
		inPlace, replaced = ip, rp
		return settled
	}, rotationSettleTimeout, 100*time.Millisecond, "CLIENT LIST must show the ORIGINAL connection re-authenticated as rotuser2 or replaced, within the pool timeout")
	t.Logf("rotation outcome: %d connection re-authenticated in place, %d replaced", inPlace, replaced)
	require.NoError(t, servedAs(ctx, admin, client, "rotuser1", "rotuser2"),
		"after the rotation the store's traffic must run as the new ACL user, whichever path the rotation took")
	require.NoError(t, op(), "the store itself serves under the new credentials")
	require.LessOrEqual(t, dials.count(), int64(2),
		"at most one further dial: the companion go-redis opens while the original re-authenticates, or the replacement of one whose re-auth stalled; more is a reconnect storm")

	// Convergence: once the re-auth cycle completes, serving ops costs no
	// further dials (an exact count during the cycle is timing-dependent; a
	// broken rotation never converges). The quiet window lets the background
	// re-auth worker win the IDLE-state race against our probing.
	require.Eventually(t, func() bool {
		return servesWithoutDialling(op, dials)
	}, 10*time.Second, 100*time.Millisecond, "the pool must converge to dial-free serving after rotation")
}

// The same acceptance criterion through the PRODUCTION wiring. The test above
// drives Refresh by hand over a hand-built client; here the Store is built by
// New() from a PasswordFile config — exactly what SelectCacheBackend does for
// a router started from YAML — so the poll watcher New() owns is the only
// thing that can notice the rewritten file. Nothing in this test calls
// Refresh: rewriting the secret is the whole operator action, and the live
// connections must re-authenticate in place, with no restart.
//
//	RESP_CACHE_TEST_VALKEY_ADDR=127.0.0.1:63790 go test ./ecosystem/cache/redisstore -run TestLiveRotationThroughConfiguredStore -v
func TestLiveRotationThroughConfiguredStore(t *testing.T) {
	addr := os.Getenv("RESP_CACHE_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("set RESP_CACHE_TEST_VALKEY_ADDR to a real Valkey/Redis (docker run --rm -p 127.0.0.1:63790:6379 valkey/valkey) to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.Ping(ctx).Err())

	for _, user := range []string{"rotwatch1", "rotwatch2"} {
		require.NoError(t, admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+user+"-pw", "~*", "+@all").Err())
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = admin.Do(cleanupCtx, "ACL", "DELUSER", "rotwatch1", "rotwatch2").Err()
	})

	credFile := writeTempFile(t, "cred-watched", "rotwatch1:rotwatch1-pw")
	store, err := New(Config{
		Addresses:                 []string{addr},
		PasswordFile:              credFile,
		CredentialRefreshInterval: 200 * time.Millisecond,
		KeyPrefix:                 "rotwatch",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// New() owns the client, so dials are counted through a hook on it and the
	// connection ids come from the server side. Without read addresses the
	// store's read client is its write client.
	dials := &dialCounter{}
	store.write.AddHook(dials)
	if store.read != store.write {
		store.read.AddHook(dials)
	}

	op := func() error { return store.SetHeight(ctx, core.HeightKey("ETH1", "0xwatched"), 1, time.Minute) }
	require.NoError(t, op(), "the store must serve under the initial credentials")
	dialsBefore := dials.count()

	established := connectionIDsForUser(ctx, t, admin, "rotwatch1")
	require.NotEmpty(t, established, "the store must hold at least one authenticated connection before the rotation")

	require.NoError(t, os.WriteFile(credFile, []byte("rotwatch2:rotwatch2-pw"), 0o600))

	// Operations keep flowing throughout; a failure is reported at once rather
	// than retried (require cannot be used inside the polled condition).
	var opErr error
	var inPlace, replaced int
	require.Eventually(t, func() bool {
		if err := op(); err != nil {
			opErr = err
			return true
		}
		settled, ip, rp := rotationSettled(ctx, t, admin, established, "rotwatch2")
		inPlace, replaced = ip, rp
		return settled
	}, rotationSettleTimeout, 250*time.Millisecond,
		"the Store's own credential watcher must get every established connection re-authenticated as the new ACL user, or replaced by one that is, within the pool timeout — no Refresh call, no restart")
	require.NoError(t, opErr, "operations must continue through the rotation")
	t.Logf("rotation outcome: %d connection(s) re-authenticated in place, %d replaced", inPlace, replaced)
	require.NoError(t, servedAs(ctx, admin, store.write, "rotwatch1", "rotwatch2"),
		"after the rotation the store's traffic must run as the new ACL user, whichever path the rotation took")
	require.NoError(t, op(), "the store itself serves under the new credentials")

	// A reconnect storm is invisible in a snapshot of live connections, which
	// only ever shows the survivors; the dial counter is cumulative.
	require.LessOrEqual(t, dials.count()-dialsBefore, int64(1),
		"at most one dial during the rotation: the companion go-redis opens while the original re-authenticates, or the replacement of one whose re-auth stalled; more is a reconnect storm")
	require.Eventually(t, func() bool {
		return servesWithoutDialling(op, dials)
	}, 10*time.Second, 100*time.Millisecond, "the pool must converge to dial-free serving after rotation")
}

// Negative control for the two proofs above. A connection that the SERVER
// drops while the credentials never change is replaced by a reconnect under
// the previous user: the original id is gone, so rotationSettled reads it as a
// settled rotation, and the operation succeeds under either user. Only the
// identity proof tells this apart from a rotation, and only the cumulative
// dial count sees repeated drops, which a snapshot of live connections cannot.
// Both proofs must reject this scenario, or they prove nothing.
func TestLiveRotationProofsRejectReplacementUnderPreviousUser(t *testing.T) {
	addr := os.Getenv("RESP_CACHE_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("set RESP_CACHE_TEST_VALKEY_ADDR to a real Valkey/Redis (docker run --rm -p 127.0.0.1:63790:6379 valkey/valkey) to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.Ping(ctx).Err())

	for _, user := range []string{"rotneg1", "rotneg2"} {
		require.NoError(t, admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+user+"-pw", "~*", "+@all").Err())
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = admin.Do(cleanupCtx, "ACL", "DELUSER", "rotneg1", "rotneg2").Err()
	})

	credFile := writeTempFile(t, "cred-negative", "rotneg1:rotneg1-pw")
	provider := NewStreamingProvider(&FileCredentials{Path: credFile})
	client := redis.NewClient(&redis.Options{Addr: addr, StreamingCredentialsProvider: provider, PoolSize: 2})
	dials := &dialCounter{}
	client.AddHook(dials)
	store, err := NewWithClient(client, "rotneg")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	op := func() error { return store.SetHeight(ctx, core.HeightKey("ETH1", "0xneg"), 1, time.Minute) }
	require.NoError(t, op())
	connID, err := client.Do(ctx, "CLIENT", "ID").Int64()
	require.NoError(t, err)
	established := map[string]bool{strconv.FormatInt(connID, 10): true}

	// No rotation: the server drops the connection and the client reconnects
	// with the credentials it still has.
	require.NoError(t, admin.Do(ctx, "CLIENT", "KILL", "ID", connID).Err())
	require.Eventually(t, func() bool { return op() == nil }, 5*time.Second, 50*time.Millisecond,
		"the client must reconnect after the server dropped its connection")

	settled, inPlace, replaced := rotationSettled(ctx, t, admin, established, "rotneg2")
	require.True(t, settled, "retirement of the original id alone reads as a settled rotation; this is the gap the identity proof closes")
	require.Equal(t, 0, inPlace)
	require.Equal(t, 1, replaced)
	err = servedAs(ctx, admin, client, "rotneg1", "rotneg2")
	require.Error(t, err, "a replacement under the previous credentials must fail the identity proof")
	require.Contains(t, err.Error(), "rotneg1")

	// Repeated drops: the live connection count stays within the pool size
	// throughout, while every drop costs a dial.
	for i := 0; i < 3; i++ {
		id, err := client.Do(ctx, "CLIENT", "ID").Int64()
		require.NoError(t, err)
		require.NoError(t, admin.Do(ctx, "CLIENT", "KILL", "ID", id).Err())
		require.Eventually(t, func() bool { return op() == nil }, 5*time.Second, 50*time.Millisecond)
	}
	require.LessOrEqual(t, len(connectionIDsForUser(ctx, t, admin, "rotneg1")), 2,
		"a snapshot of live connections cannot see the reconnects")
	require.GreaterOrEqual(t, dials.count(), int64(5),
		"the cumulative dial count can: one initial connection and one per drop")
}

// dialCounter counts the dials a go-redis client makes. Attached through
// AddHook, it works on a client the Store built itself, where no Dialer option
// can be injected.
type dialCounter struct{ dials atomic.Int64 }

func (c *dialCounter) count() int64 { return c.dials.Load() }

func (c *dialCounter) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		c.dials.Add(1)
		return next(ctx, network, addr)
	}
}

func (c *dialCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (c *dialCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// servesWithoutDialling reports whether two operations after a quiet window
// cost no dial: once the re-auth cycle completes, serving needs none (an exact
// count during the cycle is timing-dependent; a broken rotation never
// converges). The quiet window lets the background re-auth worker win the
// IDLE-state race against the probing.
func servesWithoutDialling(op func() error, dials *dialCounter) bool {
	time.Sleep(300 * time.Millisecond)
	before := dials.count()
	if op() != nil {
		return false
	}
	time.Sleep(50 * time.Millisecond)
	if op() != nil {
		return false
	}
	return dials.count() == before
}

// servedAs proves which ACL user the store's traffic runs as: ACL WHOAMI
// through the store's own client answers for a connection the pool hands out,
// and CLIENT LIST must show no connection left under the previous user and at
// least one under the new one. rotationSettled cannot tell a replacement
// dialled under the NEW credentials from a plain reconnect under the OLD ones
// (the original id is gone either way, and both users can run the operation);
// this can. It returns the violation rather than asserting, so the negative
// control can require one.
func servedAs(ctx context.Context, admin *redis.Client, client redis.UniversalClient, previousUser, newUser string) error {
	whoami, err := client.Do(ctx, "ACL", "WHOAMI").Text()
	if err != nil {
		return fmt.Errorf("ACL WHOAMI through the store's client: %w", err)
	}
	if whoami != newUser {
		return fmt.Errorf("the store's connection answers ACL WHOAMI as %q, want %q", whoami, newUser)
	}
	list, err := admin.Do(ctx, "CLIENT", "LIST").Text()
	if err != nil {
		return fmt.Errorf("CLIENT LIST: %w", err)
	}
	if previous := idsListedAs(list, previousUser); len(previous) > 0 {
		return fmt.Errorf("%d connection(s) still authenticated as %s after the rotation", len(previous), previousUser)
	}
	if len(idsListedAs(list, newUser)) == 0 {
		return fmt.Errorf("no connection authenticated as %s after the rotation", newUser)
	}
	return nil
}

// rotationSettleTimeout bounds how long a rotation may take to settle: the
// client's pool timeout, after which a connection whose re-authentication
// stalled is closed, plus a margin. These tests leave read-timeout at the
// client's default of five seconds, which makes the pool timeout six; a
// negative read-timeout (no read timeout) would make it thirty, which no test
// here configures. The bound holds because the tests keep operations flowing:
// the push only marks a connection, and go-redis starts the re-auth worker
// (and with it the pool-timeout clock) when the marked connection is next
// checked out and returned. A connection left idle after the push keeps the
// previous identity until traffic reaches it, which docs/RESP-CACHE.md says.
const rotationSettleTimeout = 15 * time.Second

// rotationSettled reports whether every connection established before a
// rotation has either been re-authenticated in place (listed under the same
// id as newUser) or been replaced (no longer listed at all). It counts each
// outcome so a run says which path it took. It only checks retirement of the
// established ids; which user the traffic runs as afterwards is servedAs's
// proof, because a replacement dialled under the previous credentials passes
// this check just the same.
//
// Both outcomes are what the client guarantees. go-redis v9.22 re-authenticates a
// pooled connection through a background worker that must first see the
// connection go idle, and that worker can miss the notification: the pool's
// Put hot path (Conn.Release, internal/pool/conn.go) marks the connection idle
// with a bare compare-and-swap that never notifies waiters, so a worker already
// parked in AwaitAndTransition (internal/pool/conn_state.go) is never woken
// (a fix is proposed upstream). The connection then serves no
// traffic (every checkout rejects it) until the worker's wait expires after the
// pool timeout, when the client closes it and dials a fresh one under the new
// credentials. About one rotation in four took that path against Valkey 8.1
// (MAG-3769, reported upstream as redis/go-redis#4027); the earlier assertion
// that the same id must re-authenticate
// failed on exactly those runs. No operation fails either way.
func rotationSettled(ctx context.Context, t *testing.T, admin *redis.Client, established map[string]bool, newUser string) (settled bool, inPlace, replaced int) {
	t.Helper()
	list, err := admin.Do(ctx, "CLIENT", "LIST").Text()
	require.NoError(t, err)
	listedAs := map[string]string{} // connection id -> ACL user
	for _, line := range strings.Split(list, "\n") {
		id, user := "", ""
		for _, field := range strings.Fields(line) {
			if value, found := strings.CutPrefix(field, "id="); found {
				id = value
			}
			if value, found := strings.CutPrefix(field, "user="); found {
				user = value
			}
		}
		if id != "" {
			listedAs[id] = user
		}
	}
	for id := range established {
		user, listed := listedAs[id]
		switch {
		case !listed:
			replaced++
		case user == newUser:
			inPlace++
		default:
			return false, 0, 0 // still authenticated as the previous user
		}
	}
	return true, inPlace, replaced
}

// connectionIDsForUser reports the server-side connection ids currently
// authenticated as user, read from CLIENT LIST.
func connectionIDsForUser(ctx context.Context, t *testing.T, admin *redis.Client, user string) map[string]bool {
	t.Helper()
	list, err := admin.Do(ctx, "CLIENT", "LIST").Text()
	require.NoError(t, err)
	return idsListedAs(list, user)
}

// idsListedAs extracts from a CLIENT LIST reply the ids of the connections
// authenticated as user.
func idsListedAs(list, user string) map[string]bool {
	ids := map[string]bool{}
	for _, line := range strings.Split(list, "\n") {
		if !strings.Contains(line, " user="+user+" ") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if id, found := strings.CutPrefix(field, "id="); found {
				ids[id] = true
			}
		}
	}
	return ids
}
