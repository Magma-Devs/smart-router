package rpcsmartrouter

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

var _ grpcproxy.ReflectionSource = (*RPCSmartRouterServer)(nil)

// errNoReflectionSnapshotters is the reflection answer for a chain with no live
// gRPC endpoint to take a snapshot from.
var errNoReflectionSnapshotters = errors.New("no live gRPC endpoint to take a reflection snapshot from")

// ReflectionSnapshot implements grpcproxy.ReflectionSource.
func (rpcss *RPCSmartRouterServer) ReflectionSnapshot(ctx context.Context) (grpcproxy.ReflectionSnapshot, error) {
	if rpcss.sessionManager == nil {
		return nil, errNoReflectionSnapshotters
	}
	primaries, backups := orderSnapshotters(rpcss.sessionManager.GetAllDirectRPCEndpoints())
	snapshot, err := pickReflectionSnapshot(ctx, primaries, backups)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// orderSnapshotters splits the endpoints' gRPC connections into primaries and
// backups, each in provider then address order, so the same endpoint is preferred
// every time.
func orderSnapshotters(endpoints []*lavasession.EndpointWithDirectConnection) (primaries, backups []lavasession.GRPCReflectionSnapshotter) {
	endpoints = slices.Clone(endpoints)
	slices.SortFunc(endpoints, func(a, b *lavasession.EndpointWithDirectConnection) int {
		return cmp.Or(cmp.Compare(a.ProviderAddress, b.ProviderAddress),
			cmp.Compare(a.Endpoint.NetworkAddress, b.Endpoint.NetworkAddress))
	})
	for _, endpoint := range endpoints {
		snapshotter, ok := endpoint.DirectConnection.(lavasession.GRPCReflectionSnapshotter)
		if !ok {
			continue
		}
		if endpoint.Backup {
			backups = append(backups, snapshotter)
		} else {
			primaries = append(primaries, snapshotter)
		}
	}
	return primaries, backups
}

// pickReflectionSnapshot chooses the snapshot a reflection stream is answered from.
// The first current one in endpoint order wins, so consecutive streams get the same
// build, and nothing else is refreshed. With none current it serves the best one
// held while the primaries and the served endpoint refresh; with none held it waits
// for the first to be taken.
func pickReflectionSnapshot(ctx context.Context, primaries, backups []lavasession.GRPCReflectionSnapshotter) (*lavasession.GRPCReflectionSnapshot, error) {
	var best *lavasession.GRPCReflectionSnapshot
	var bestFrom lavasession.GRPCReflectionSnapshotter
	for _, snapshotter := range slices.Concat(primaries, backups) {
		held := snapshotter.PeekReflectionSnapshot()
		if held == nil {
			continue
		}
		if held.Current() {
			return held, nil
		}
		if best == nil || betterSnapshot(held, best) {
			best, bestFrom = held, snapshotter
		}
	}
	if best == nil {
		return awaitFirstSnapshot(ctx, primaries, backups)
	}
	bestFrom.ReflectionSnapshot()
	for _, primary := range primaries {
		primary.ReflectionSnapshot()
	}
	return best, nil
}

// betterSnapshot reports whether a beats b: complete before partial, then newer.
func betterSnapshot(a, b *lavasession.GRPCReflectionSnapshot) bool {
	if a.Complete != b.Complete {
		return a.Complete
	}
	return a.Taken.After(b.Taken)
}

// awaitFirstSnapshot waits for the first snapshot any endpoint takes. The backups
// join the primaries once half of ctx's time has passed or every primary has
// failed, so a hung primary cannot starve them.
func awaitFirstSnapshot(ctx context.Context, primaries, backups []lavasession.GRPCReflectionSnapshotter) (*lavasession.GRPCReflectionSnapshot, error) {
	if len(primaries)+len(backups) == 0 {
		return nil, errNoReflectionSnapshotters
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		snapshot *lavasession.GRPCReflectionSnapshot
		err      error
	}
	results := make(chan result, len(primaries)+len(backups))
	pending := 0
	ask := func(tier []lavasession.GRPCReflectionSnapshotter) {
		for _, snapshotter := range tier {
			pending++
			go func() {
				snapshot, err := snapshotter.AwaitReflectionSnapshot(ctx)
				results <- result{snapshot, err}
			}()
		}
	}
	askBackups := func() {
		ask(backups)
		backups = nil
	}

	var halfway <-chan time.Time
	if deadline, ok := ctx.Deadline(); ok && len(backups) > 0 {
		halfway = time.After(time.Until(deadline) / 2)
	}
	ask(primaries)
	if pending == 0 {
		askBackups()
	}

	lastErr := errNoReflectionSnapshotters
	for pending > 0 {
		select {
		case r := <-results:
			pending--
			if r.err == nil {
				return r.snapshot, nil
			}
			lastErr = r.err
			if pending == 0 {
				askBackups()
			}
		case <-halfway:
			halfway = nil
			askBackups()
		}
	}
	return nil, lastErr
}
