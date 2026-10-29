package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// subscriptionManagerCase is one subscription manager as its endpoint cascade sees it. The
// WebSocket and gRPC managers each carry their own selectEndpoint/selectFromTier, so each
// needs the whole matrix below; with one test file per manager, each had only part of it.
type subscriptionManagerCase struct {
	name string
	// url turns an endpoint name from the table into this manager's URL form.
	url func(endpoint string) string
	// build makes a manager over the given tiers. optimizer is nil for first-available.
	build func(primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionCascade
}

// subscriptionCascade is what the table reads off a built manager.
type subscriptionCascade struct {
	selectEndpoint func(ctx context.Context, clientKey string, ignored map[string]struct{}) (*common.NodeUrl, error)
	stickyStore    *lavasession.StickySessionStore
	endpointsByURL map[string]*common.NodeUrl
}

func subscriptionManagerCases() []subscriptionManagerCase {
	return []subscriptionManagerCase{
		{
			name: "websocket",
			url:  func(endpoint string) string { return "wss://" + endpoint + ".example.com" },
			build: func(primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionCascade {
				manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", "ETH", "jsonrpc", primary, backup, optimizer, nil)
				return subscriptionCascade{manager.selectEndpoint, manager.stickyStore, manager.endpointsByURL}
			},
		},
		{
			name: "grpc",
			url:  func(endpoint string) string { return "grpc://" + endpoint + ".example.com:9090" },
			build: func(primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionCascade {
				manager := NewDirectGRPCSubscriptionManager(nil, "ETH", "grpc", primary, backup, optimizer, nil)
				return subscriptionCascade{manager.selectEndpoint, manager.stickyStore, manager.endpointsByURL}
			},
		},
	}
}

// TestSelectEndpoint_Cascade runs the sticky → primary → backup cascade matrix against both
// subscription managers. Endpoints are named; each manager's url func gives them its scheme.
//
// Every row also checks the sticky store afterwards: selectEndpoint must never write it,
// because the write belongs to createNewSubscription once the upstream connection is
// verified. A sticky entry written here would pin a client to a primary that then fails to
// connect, and the cascade would never reach the backup tier.
func TestSelectEndpoint_Cascade(t *testing.T) {
	type scenario struct {
		name    string
		primary []string
		backup  []string
		// emptyBackup passes a non-nil empty backup slice, which must behave as nil.
		emptyBackup bool
		// sticky pre-seeds the client's sticky session.
		sticky  string
		ignored []string
		// pick, when set, is the optimizer: it gets the tier's endpoint names and returns
		// the chosen ones. Unset means no optimizer.
		pick func(tier []string) []string

		want string // empty means selectEndpoint must fail
		// wantOptimizerCalls is how many tiers reached the optimizer.
		wantOptimizerCalls int
		// wantStickyAfter is the sticky session left behind; empty means none.
		wantStickyAfter string
	}

	pickIfPresent := func(endpoint string) func([]string) []string {
		return func(tier []string) []string {
			for _, name := range tier {
				if name == endpoint {
					return []string{endpoint}
				}
			}
			return nil
		}
	}

	scenarios := []scenario{
		{name: "primary only, nil backup: first primary", primary: []string{"primary-1", "primary-2"}, want: "primary-1"},
		{name: "primary only, empty backup: same as nil", primary: []string{"primary-1"}, emptyBackup: true, want: "primary-1"},
		{name: "primary exhausted: falls back to backup", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, ignored: []string{"primary-1", "primary-2"}, want: "backup-1"},
		{name: "primary empty: first backup", backup: []string{"backup-1", "backup-2"}, want: "backup-1"},
		{name: "both exhausted: error", primary: []string{"primary-1"}, backup: []string{"backup-1"}, ignored: []string{"primary-1", "backup-1"}},
		{name: "both empty: error"},
		{name: "first primary ignored: second primary, not backup", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, ignored: []string{"primary-1"}, want: "primary-2"},

		{name: "sticky on primary wins over first-available", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, sticky: "primary-2", want: "primary-2", wantStickyAfter: "primary-2"},
		{name: "sticky on backup resolves across tiers", primary: []string{"primary-1"}, backup: []string{"backup-1"}, sticky: "backup-1", want: "backup-1", wantStickyAfter: "backup-1"},
		{name: "ignored sticky is cleared and the cascade runs", primary: []string{"primary-1"}, backup: []string{"backup-1"}, sticky: "primary-1", ignored: []string{"primary-1"}, want: "backup-1"},
		{name: "sticky on an unknown endpoint is kept and skipped", primary: []string{"primary-1"}, sticky: "gone-1", want: "primary-1", wantStickyAfter: "gone-1"},

		{name: "optimizer picks within primary, backup never consulted", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1", "backup-2"}, pick: pickIfPresent("primary-2"), want: "primary-2", wantOptimizerCalls: 1},
		{name: "optimizer empty over primary falls back to first primary", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1", "backup-2"}, pick: pickIfPresent("backup-2"), want: "primary-1", wantOptimizerCalls: 1},
		{name: "optimizer over backup once primary is ignored", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1", "backup-2"}, ignored: []string{"primary-1", "primary-2"}, pick: pickIfPresent("backup-2"), want: "backup-2", wantOptimizerCalls: 2},
		{name: "optimizer over backup when primary is empty", backup: []string{"backup-1", "backup-2"}, pick: pickIfPresent("backup-2"), want: "backup-2", wantOptimizerCalls: 1},
		{name: "single-endpoint tier skips the optimizer", primary: []string{"primary-1"}, pick: pickIfPresent("primary-1"), want: "primary-1"},
	}

	for _, manager := range subscriptionManagerCases() {
		t.Run(manager.name, func(t *testing.T) {
			nodeURLs := func(names []string) []*common.NodeUrl {
				if names == nil {
					return nil
				}
				urls := make([]*common.NodeUrl, 0, len(names))
				for _, name := range names {
					urls = append(urls, &common.NodeUrl{Url: manager.url(name)})
				}
				return urls
			}

			for _, sc := range scenarios {
				t.Run(sc.name, func(t *testing.T) {
					backup := nodeURLs(sc.backup)
					if sc.emptyBackup {
						backup = []*common.NodeUrl{}
					}

					// An untyped nil, not a nil *fakeSubscriptionOptimizer: the managers
					// test the interface against nil to choose first-available.
					var optimizer WebSocketEndpointOptimizer
					optimizerCalls := 0
					if sc.pick != nil {
						nameOf := map[string]string{}
						for _, name := range append(append([]string{}, sc.primary...), sc.backup...) {
							nameOf[manager.url(name)] = name
						}
						optimizer = &fakeSubscriptionOptimizer{chooseFn: func(addresses []string, _ map[string]struct{}) []string {
							optimizerCalls++
							names := make([]string, 0, len(addresses))
							for _, address := range addresses {
								names = append(names, nameOf[address])
							}
							picked := sc.pick(names)
							urls := make([]string, 0, len(picked))
							for _, name := range picked {
								urls = append(urls, manager.url(name))
							}
							return urls
						}}
					}

					cascade := manager.build(nodeURLs(sc.primary), backup, optimizer)
					if sc.sticky != "" {
						cascade.stickyStore.Set("client-1", &lavasession.StickySession{Provider: manager.url(sc.sticky)})
					}
					var ignored map[string]struct{}
					if sc.ignored != nil {
						ignored = map[string]struct{}{}
						for _, name := range sc.ignored {
							ignored[manager.url(name)] = struct{}{}
						}
					}

					ep, err := cascade.selectEndpoint(context.Background(), "client-1", ignored)
					if sc.want == "" {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						assert.Equal(t, manager.url(sc.want), ep.Url)
					}
					assert.Equal(t, sc.wantOptimizerCalls, optimizerCalls, "tiers that reached the optimizer")

					sticky, exists := cascade.stickyStore.Get("client-1")
					if sc.wantStickyAfter == "" {
						assert.False(t, exists, "selectEndpoint must not leave a sticky session behind")
					} else {
						require.True(t, exists)
						assert.Equal(t, manager.url(sc.wantStickyAfter), sticky.Provider)
					}
				})
			}
		})
	}
}

func TestSelectEndpoint_EndpointsByURLSpansBothTiers(t *testing.T) {
	for _, manager := range subscriptionManagerCases() {
		t.Run(manager.name, func(t *testing.T) {
			primary := []*common.NodeUrl{{Url: manager.url("primary-1")}}
			backup := []*common.NodeUrl{{Url: manager.url("backup-1")}}
			cascade := manager.build(primary, backup, nil)

			require.Len(t, cascade.endpointsByURL, 2)
			assert.Same(t, primary[0], cascade.endpointsByURL[manager.url("primary-1")])
			assert.Same(t, backup[0], cascade.endpointsByURL[manager.url("backup-1")])
		})
	}
}
