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

func TestApplyReverification(t *testing.T) {
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
			}

			got, demoted := applyReverification(context.Background(), inputs, fresh, reverifyTierStatic, uint64(42), validate)
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
	}

	_, _ = applyReverification(context.Background(), inputs, map[uint64]*lavasession.ConsumerSessionsWithProvider{}, reverifyTierBackup, 1, validate)
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
