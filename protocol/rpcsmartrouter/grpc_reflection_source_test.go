package rpcsmartrouter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// fakeSnapshotter holds a snapshot, or takes one: fresh on success, failing with
// err, or with hang set waiting out ctx like an endpoint that never answers. It
// counts the calls that may start a refresh.
type fakeSnapshotter struct {
	mu        sync.Mutex
	held      *lavasession.GRPCReflectionSnapshot
	fresh     *lavasession.GRPCReflectionSnapshot
	err       error
	hang      bool
	refreshes atomic.Int32
	awaits    atomic.Int32
}

func (f *fakeSnapshotter) PeekReflectionSnapshot() *lavasession.GRPCReflectionSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held
}

func (f *fakeSnapshotter) ReflectionSnapshot() *lavasession.GRPCReflectionSnapshot {
	f.refreshes.Add(1)
	return f.PeekReflectionSnapshot()
}

func (f *fakeSnapshotter) AwaitReflectionSnapshot(ctx context.Context) (*lavasession.GRPCReflectionSnapshot, error) {
	f.awaits.Add(1)
	if held := f.PeekReflectionSnapshot(); held != nil {
		return held, nil
	}
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held = f.fresh
	return f.fresh, nil
}

func snapshotNamed(name string, complete bool, age time.Duration) *lavasession.GRPCReflectionSnapshot {
	return &lavasession.GRPCReflectionSnapshot{Services: []string{name}, Complete: complete, Taken: time.Now().Add(-age)}
}

func snapshotters(fakes ...*fakeSnapshotter) []lavasession.GRPCReflectionSnapshotter {
	out := make([]lavasession.GRPCReflectionSnapshotter, 0, len(fakes))
	for _, fake := range fakes {
		out = append(out, fake)
	}
	return out
}

func TestPickReflectionSnapshot_TheFirstCurrentInOrderWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cold := &fakeSnapshotter{fresh: snapshotNamed("cold", true, 0)}
	stale := &fakeSnapshotter{held: snapshotNamed("stale", true, 72*time.Hour)}
	current := &fakeSnapshotter{held: snapshotNamed("current", true, 0)}
	alsoCurrent := &fakeSnapshotter{held: snapshotNamed("also-current", true, 0)}

	for range 3 {
		snapshot, err := pickReflectionSnapshot(ctx, snapshotters(cold, stale, current, alsoCurrent), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"current"}, snapshot.Services,
			"a current snapshot beats a stale one ahead of it, and the order settles ties")
	}
	for _, other := range []*fakeSnapshotter{cold, stale, current, alsoCurrent} {
		require.Zero(t, other.refreshes.Load()+other.awaits.Load(),
			"while a snapshot is current, no endpoint is refreshed or waited on")
	}
}

// With none current the served endpoint and the primaries refresh, so a stale
// preferred endpoint has a way out; a backup that is not being served does not.
func TestPickReflectionSnapshot_WithNoneCurrentOnlyPrimariesAndTheServedRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stale := &fakeSnapshotter{held: snapshotNamed("stale", true, time.Hour)}
	empty := &fakeSnapshotter{}
	olderBackup := &fakeSnapshotter{held: snapshotNamed("older-backup", true, 2*time.Hour)}

	snapshot, err := pickReflectionSnapshot(ctx, snapshotters(stale, empty), snapshotters(olderBackup))
	require.NoError(t, err)
	require.Equal(t, []string{"stale"}, snapshot.Services)
	require.Positive(t, stale.refreshes.Load())
	require.Positive(t, empty.refreshes.Load(), "a primary with nothing held is given the chance to take one")
	require.Zero(t, olderBackup.refreshes.Load()+olderBackup.awaits.Load(), "a backup not being served is left alone")
}

func TestPickReflectionSnapshot_WithNoneCurrentServesTheBestHeld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	partial := &fakeSnapshotter{held: snapshotNamed("partial", false, 0)}
	olderComplete := &fakeSnapshotter{held: snapshotNamed("older-complete", true, time.Hour)}
	snapshot, err := pickReflectionSnapshot(ctx, snapshotters(partial, olderComplete), nil)
	require.NoError(t, err)
	require.Equal(t, []string{"older-complete"}, snapshot.Services, "complete beats partial")

	oldest := &fakeSnapshotter{held: snapshotNamed("oldest", true, 72*time.Hour)}
	older := &fakeSnapshotter{held: snapshotNamed("older", true, time.Hour)}
	snapshot, err = pickReflectionSnapshot(ctx, snapshotters(oldest, older), nil)
	require.NoError(t, err)
	require.Equal(t, []string{"older"}, snapshot.Services, "then newer beats older")
}

func TestPickReflectionSnapshot_ColdStartTakesTheFirstToFinish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hung := &fakeSnapshotter{hang: true}
	answers := &fakeSnapshotter{fresh: snapshotNamed("answers", true, 0)}

	started := time.Now()
	snapshot, err := pickReflectionSnapshot(ctx, snapshotters(hung, answers), nil)
	require.NoError(t, err)
	require.Equal(t, []string{"answers"}, snapshot.Services)
	require.Less(t, time.Since(started), time.Second, "a hung endpoint must not hold up one that answers")
}

func TestPickReflectionSnapshot_BackupsJoinHalfwayThroughAHungPrimary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	hung := &fakeSnapshotter{hang: true}
	backup := &fakeSnapshotter{fresh: snapshotNamed("backup", true, 0)}

	started := time.Now()
	snapshot, err := pickReflectionSnapshot(ctx, snapshotters(hung), snapshotters(backup))
	require.NoError(t, err)
	require.Equal(t, []string{"backup"}, snapshot.Services)
	require.Less(t, time.Since(started), 1800*time.Millisecond)
}

func TestPickReflectionSnapshot_BackupsWaitWhileAPrimaryAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	healthy := &fakeSnapshotter{fresh: snapshotNamed("primary", true, 0)}
	spare := &fakeSnapshotter{fresh: snapshotNamed("spare", true, 0)}
	snapshot, err := pickReflectionSnapshot(ctx, snapshotters(healthy), snapshotters(spare))
	require.NoError(t, err)
	require.Equal(t, []string{"primary"}, snapshot.Services)
	require.Zero(t, spare.awaits.Load(), "a backup is not asked while a primary answers")

	failed := &fakeSnapshotter{err: errors.New("reflection unavailable")}
	backup := &fakeSnapshotter{fresh: snapshotNamed("backup", true, 0)}
	snapshot, err = pickReflectionSnapshot(ctx, snapshotters(failed), snapshotters(backup))
	require.NoError(t, err)
	require.Equal(t, []string{"backup"}, snapshot.Services, "a failed primary brings the backups in at once")
}

func TestPickReflectionSnapshot_ReportsFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pickReflectionSnapshot(ctx, nil, nil)
	require.ErrorIs(t, err, errNoReflectionSnapshotters)

	unavailable := errors.New("reflection unavailable")
	_, err = pickReflectionSnapshot(ctx,
		snapshotters(&fakeSnapshotter{err: unavailable}),
		snapshotters(&fakeSnapshotter{err: unavailable}))
	require.ErrorIs(t, err, unavailable)
}

// The preference is only stable if the order is: endpoints come back from a map, so
// the order has to be imposed here, by provider and then by address.
func TestOrderSnapshotters_ByTierThenProviderThenAddress(t *testing.T) {
	ctx := context.Background()
	endpoint := func(provider, address string, backup bool) *lavasession.EndpointWithDirectConnection {
		conn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: "grpcs://" + address + ":443"}, 5, "")
		require.NoError(t, err)
		return &lavasession.EndpointWithDirectConnection{
			Endpoint:         &lavasession.Endpoint{NetworkAddress: address},
			DirectConnection: conn,
			ProviderAddress:  provider,
			Backup:           backup,
		}
	}
	bB := endpoint("bravo", "b.example", false)
	aZ := endpoint("alpha", "z.example", false)
	aA := endpoint("alpha", "a.example", false)
	backup := endpoint("aaa-backup", "c.example", true)

	snapshotterOf := func(e *lavasession.EndpointWithDirectConnection) lavasession.GRPCReflectionSnapshotter {
		snapshotter, ok := e.DirectConnection.(lavasession.GRPCReflectionSnapshotter)
		require.True(t, ok)
		return snapshotter
	}

	primaries, backups := orderSnapshotters([]*lavasession.EndpointWithDirectConnection{bB, backup, aZ, aA})
	require.Equal(t, []lavasession.GRPCReflectionSnapshotter{snapshotterOf(aA), snapshotterOf(aZ), snapshotterOf(bB)}, primaries)
	require.Equal(t, []lavasession.GRPCReflectionSnapshotter{snapshotterOf(backup)}, backups)
}
