package rpcsmartrouter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

func makeSession(name string) *lavasession.ConsumerSessionsWithProvider {
	return lavasession.NewConsumerSessionWithProvider(name, nil, 1, 1, 0)
}

func makeProvider(name string) *lavasession.RPCStaticProviderEndpoint {
	return &lavasession.RPCStaticProviderEndpoint{Name: name, ApiInterface: "rest"}
}

// fakeConvert mimics the closure built in CreateSmartRouterEndpoint: turn a
// list of providers into a session map. Used only by the promote path.
func fakeConvert(p []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
	out := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i, ep := range p {
		out[uint64(i)] = makeSession(ep.Name)
	}
	return out
}

func collectNames(m map[uint64]*lavasession.ConsumerSessionsWithProvider) map[string]struct{} {
	out := map[string]struct{}{}
	for _, s := range m {
		out[s.PublicLavaAddress] = struct{}{}
	}
	return out
}

// withImmediateDemote pins reverifyDemoteThreshold to 1 (demote on the first failed cycle) for
// the duration of a test, restoring the production value on cleanup.
//
// The demote-orchestration tests predate the MAG-2445 hysteresis and are about what the
// reconciliation DOES with a demote — which map it lands in, whether updateEpoch stores it,
// whether the session manager sees it — not about how many failed cycles earn one. Driving two
// cycles in each of them would add noise unrelated to their subject. The threshold itself is
// pinned by TestApplyReverification_DemoteHysteresis.
func withImmediateDemote(t *testing.T) {
	t.Helper()
	restore := reverifyDemoteThreshold
	reverifyDemoteThreshold = 1
	t.Cleanup(func() { reverifyDemoteThreshold = restore })
}

func TestApplyReverification(t *testing.T) {
	withImmediateDemote(t)
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "rest"}

	tests := []struct {
		name        string
		configured  []string
		fresh       []string        // names already present in the freshened active map
		failing     map[string]bool // by provider name; absent => passes validate
		want        []string        // expected names in the returned map
		wantAdmits  []string        // names that must come from convertProvidersToSessions (promotions)
		wantDemoted []string        // names that must surface in the demoted return slice
	}{
		{
			name:       "steady-state healthy",
			configured: []string{"A", "B"},
			fresh:      []string{"A", "B"},
			want:       []string{"A", "B"},
		},
		{
			name:       "promote: failed-init now passing",
			configured: []string{"A", "B"},
			fresh:      []string{"A"},
			want:       []string{"A", "B"},
			wantAdmits: []string{"B"},
		},
		{
			name:        "demote: active now failing",
			configured:  []string{"A", "B"},
			fresh:       []string{"A", "B"},
			failing:     map[string]bool{"B": true},
			want:        []string{"A"},
			wantDemoted: []string{"B"},
		},
		{
			name:       "still-failing failed-init stays out",
			configured: []string{"A", "B"},
			fresh:      []string{"A"},
			failing:    map[string]bool{"B": true},
			want:       []string{"A"},
		},
		{
			name:        "mixed: promote + demote in one cycle",
			configured:  []string{"A", "B", "C"},
			fresh:       []string{"A", "B"},
			failing:     map[string]bool{"B": true},
			want:        []string{"A", "C"},
			wantAdmits:  []string{"C"},
			wantDemoted: []string{"B"},
		},
		{
			name:        "all configured failing wipes fresh",
			configured:  []string{"A", "B"},
			fresh:       []string{"A", "B"},
			failing:     map[string]bool{"A": true, "B": true},
			want:        nil,
			wantDemoted: []string{"A", "B"},
		},
		{
			name:       "empty configured returns fresh unchanged",
			configured: nil,
			fresh:      []string{"A"},
			want:       []string{"A"},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fresh := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
			freshByName := map[string]*lavasession.ConsumerSessionsWithProvider{}
			for j, n := range tt.fresh {
				s := makeSession(n)
				fresh[uint64(j)] = s
				freshByName[n] = s
			}
			configured := make([]*lavasession.RPCStaticProviderEndpoint, len(tt.configured))
			for j, n := range tt.configured {
				configured[j] = makeProvider(n)
			}

			// Track what convertProvidersToSessions is called with — promotions must
			// flow through it; survivors must not.
			var convertCalls []string
			convert := func(p []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
				for _, ep := range p {
					convertCalls = append(convertCalls, ep.Name)
				}
				return fakeConvert(p)
			}

			validate := func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
				if tt.failing[p.Name] {
					return errors.New("mock failure")
				}
				return nil
			}

			inputs := &chainReverifyInputs{
				rpcEndpoint:                rpc,
				convertProvidersToSessions: convert,
				configuredStatic:           configured,
				validateFn:                 validate,
			}

			got, demoted, _ := applyReverification(context.Background(), inputs, fresh, reverifyTierStatic, uint64(42))
			gotNames := collectNames(got)

			require.Len(t, gotNames, len(tt.want), "result size, tc #%d", i)
			for _, n := range tt.want {
				require.Contains(t, gotNames, n, "missing expected provider %q, tc #%d", n, i)
			}

			require.ElementsMatch(t, tt.wantAdmits, convertCalls, "convertProvidersToSessions call set, tc #%d", i)

			demotedNames := make([]string, 0, len(demoted))
			for _, s := range demoted {
				demotedNames = append(demotedNames, s.PublicLavaAddress)
			}
			require.ElementsMatch(t, tt.wantDemoted, demotedNames, "demoted set, tc #%d", i)

			// Survivors must keep their original session pointer (no recreation).
			admits := map[string]struct{}{}
			for _, n := range tt.wantAdmits {
				admits[n] = struct{}{}
			}
			gotByName := map[string]*lavasession.ConsumerSessionsWithProvider{}
			for _, s := range got {
				gotByName[s.PublicLavaAddress] = s
			}
			for n := range gotByName {
				if _, isAdmit := admits[n]; isAdmit {
					continue
				}
				require.Same(t, freshByName[n], gotByName[n], "survivor %q must reuse the fresh session pointer, tc #%d", n, i)
			}

			// Promoted sessions must carry the current epoch.
			for n := range admits {
				require.Equal(t, uint64(42), gotByName[n].PairingEpoch, "promoted %q must carry current epoch, tc #%d", n, i)
			}
		})
	}
}

// TestApplyReverification_BackupTierReadsBackupList confirms the typed-tier
// switch picks inputs.configuredBackup (not configuredStatic) when invoked
// with reverifyTierBackup. The static path is exercised by the table tests
// above; this ensures the discriminator actually routes.
func TestApplyReverification_BackupTierReadsBackupList(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "rest"}
	staticOnly := []*lavasession.RPCStaticProviderEndpoint{makeProvider("S")}
	backupOnly := []*lavasession.RPCStaticProviderEndpoint{makeProvider("B")}

	var calls []string
	validate := func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
		calls = append(calls, p.Name)
		return nil
	}

	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           staticOnly,
		configuredBackup:           backupOnly,
		validateFn:                 validate,
	}

	_, _, _ = applyReverification(context.Background(), inputs, map[uint64]*lavasession.ConsumerSessionsWithProvider{}, reverifyTierBackup, 1)
	require.Equal(t, []string{"B"}, calls, "backup tier must validate the backup list")
}

func TestByName(t *testing.T) {
	sessions := map[uint64]*lavasession.ConsumerSessionsWithProvider{
		3: makeSession("X"),
		7: makeSession("Y"),
	}
	got := byName(sessions)
	require.Len(t, got, 2)
	require.Contains(t, got, "X")
	require.Contains(t, got, "Y")
}

// TestValidateProvider_SmokeWiring exercises validateProvider end-to-end:
// clone-isolation lookup, GetChainRouter, ChainFetcher construction, and
// Validate dispatch. It uses an empty REST spec (so GetVerifications returns
// nothing and Validate succeeds without hitting the wire), which means a
// fully-broken function — wrong chainParser threading, GetChainRouter
// signature drift, NewChainFetcher option struct mismatch, etc. — would fail
// here even though no real network probe is performed. The cancellable-ctx
// argument ensures a regression that ignored ctx couldn't pass the timeout
// bound silently.
//
// A deeper hung-server / cancellation-propagation test would require a Spec
// with at least one Verification triggering FetchLatestBlockNum, which is
// significantly more setup for marginal additional coverage over what the
// applyReverification table-tests already provide.
func TestValidateProvider_SmokeWiring(t *testing.T) {
	parser, err := chainlib.NewRestChainParser()
	require.NoError(t, err)
	parser.SetSpec(spectypes.Spec{})

	provider := &lavasession.RPCStaticProviderEndpoint{
		ChainID:      "TEST",
		ApiInterface: "rest",
		NodeUrls:     []common.NodeUrl{{Url: "http://127.0.0.1:1"}},
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		_ = validateProvider(context.Background(), provider, parser, 2*time.Second)
	}()

	select {
	case <-done:
		require.Less(t, time.Since(start), 3*time.Second, "validateProvider must complete within the timeout bound")
	case <-time.After(5 * time.Second):
		t.Fatal("validateProvider hung past its timeout argument — wiring regression")
	}
}

// TestApplyReverification_DemoteHysteresis is the MAG-2445 pin: an active provider that fails a
// single re-verify cycle keeps its place in the pairing, and only a failure that PERSISTS across
// reverifyDemoteThreshold consecutive cycles demotes it.
//
// The bug this closes: epochs are wall-clock derived (floor((now-2024-01-01)/15m) in
// common.EpochTimer), so boundaries land on :00/:15/:30/:45 regardless of process uptime. A blip
// of a few tens of seconds that happened to overlap one tick demoted the provider outright, which
// cost it a full epoch out of the pairing — and, before MAG-2622, its ChainTracker never came
// back after the eventual promote either. Validate's own 3x retry does not help: those attempts
// run back-to-back with no delay, so all three land inside the same outage.
func TestApplyReverification_DemoteHysteresis(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "rest"}
	configured := []*lavasession.RPCStaticProviderEndpoint{makeProvider("A"), makeProvider("B")}

	// One inputs value reused across cycles — demoteFailStreak living there is exactly what
	// carries the failure count from one epoch tick to the next.
	failing := map[string]bool{}
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           configured,
		validateFn: func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			if failing[p.Name] {
				return errors.New("mock outage")
			}
			return nil
		},
	}

	// cycle runs one epoch tick over a pairing containing exactly `present`, and reports which
	// providers survived and which were demoted.
	cycle := func(epoch uint64, present ...string) (survived map[string]struct{}, demoted []string) {
		fresh := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
		for i, n := range present {
			fresh[uint64(i)] = makeSession(n)
		}
		got, demotedSessions, _ := applyReverification(context.Background(), inputs, fresh, reverifyTierStatic, epoch)
		for _, s := range demotedSessions {
			demoted = append(demoted, s.PublicLavaAddress)
		}
		return collectNames(got), demoted
	}

	require.Equal(t, 2, reverifyDemoteThreshold, "this test is written against the production threshold")

	// Cycle 1 — B goes down mid-epoch. It must be KEPT: this is the blip.
	failing["B"] = true
	survived, demoted := cycle(1, "A", "B")
	require.Contains(t, survived, "B", "a provider failing its FIRST re-verify cycle must stay paired (MAG-2445)")
	require.Empty(t, demoted, "no demotion on the first failed cycle")

	// Cycle 2 — still down. The failure has now persisted across two boundaries, so it goes.
	survived, demoted = cycle(2, "A", "B")
	require.NotContains(t, survived, "B", "a failure persisting across the threshold must demote")
	require.Equal(t, []string{"B"}, demoted, "the demoted session must be surfaced for connection teardown")
	require.Contains(t, survived, "A", "the healthy provider is untouched throughout")

	// A recovered blip must not accumulate toward a later, unrelated one. B is re-promoted on
	// recovery, then fails once more — that single failure starts a FRESH grace budget rather
	// than tipping it straight back out.
	delete(failing, "B")
	survived, _ = cycle(3, "A")
	require.Contains(t, survived, "B", "recovered provider is promoted back")

	failing["B"] = true
	survived, demoted = cycle(4, "A", "B")
	require.Contains(t, survived, "B",
		"the streak must reset on success — a later isolated failure gets the full grace budget, not a carried-over count")
	require.Empty(t, demoted)
}

// A success between two failures resets the counter, so an endpoint that flaps every other cycle
// is never demoted by accumulation. Separated from the walk above because it pins the reset rule
// specifically: without the delete-on-success the two failures would sum to the threshold.
func TestApplyReverification_SuccessResetsDemoteStreak(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "rest"}
	configured := []*lavasession.RPCStaticProviderEndpoint{makeProvider("A")}

	var fail bool
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           configured,
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			if fail {
				return errors.New("mock outage")
			}
			return nil
		},
	}

	run := func(epoch uint64) map[string]struct{} {
		fresh := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("A")}
		got, _, _ := applyReverification(context.Background(), inputs, fresh, reverifyTierStatic, epoch)
		return collectNames(got)
	}

	fail = true
	require.Contains(t, run(1), "A", "first failure is grace")
	fail = false
	require.Contains(t, run(2), "A", "recovered")
	fail = true
	require.Contains(t, run(3), "A",
		"the intervening success must have cleared the streak — otherwise this second failure demotes")
}
