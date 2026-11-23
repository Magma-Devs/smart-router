package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// subscriptionManagerCase is one subscription manager as its endpoint cascade sees it. Both
// managers select through the shared subscriptionEndpoints, each built with its own error text
// and hold-off registry, so the whole matrix below runs against each manager as constructed.
type subscriptionManagerCase struct {
	name string
	// url turns an endpoint name from the table into this manager's URL form.
	url func(endpoint string) string
	// build makes a manager over the given tiers. optimizer is nil for first-available.
	build func(primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionCascade
	// exhausted is the error this manager returns when neither tier has an endpoint to give.
	exhausted string
}

// subscriptionCascade is what the table reads off a built manager.
type subscriptionCascade struct {
	selectEndpoint func(ctx context.Context, clientKey string) (*common.NodeUrl, error)
	stickyStore    *routersession.StickySessionStore
	endpointsByURL map[string]*common.NodeUrl
	setURLLendRank func(lendRank func(url string) int)
}

func subscriptionManagerCases() []subscriptionManagerCase {
	return []subscriptionManagerCase{
		{
			name: "websocket",
			url:  func(endpoint string) string { return "wss://" + endpoint + ".example.com" },
			build: func(primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionCascade {
				manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", "ETH", "jsonrpc", primary, backup, optimizer, nil)
				return subscriptionCascade{manager.selectEndpoint, manager.stickyStore, manager.endpointsByURL, manager.SetURLLendRank}
			},
			exhausted: "no WebSocket endpoints available (primary and backup both exhausted)",
		},
		{
			name: "grpc",
			url:  func(endpoint string) string { return "grpc://" + endpoint + ".example.com:9090" },
			build: func(primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionCascade {
				manager := NewDirectGRPCSubscriptionManager(nil, "ETH", "grpc", primary, backup, optimizer, nil)
				return subscriptionCascade{manager.selectEndpoint, manager.stickyStore, manager.endpointsByURL, manager.SetURLLendRank}
			},
			exhausted: "no gRPC endpoints available",
		},
	}
}

// TestSelectEndpoint_Cascade runs the sticky → primary → backup cascade matrix against both
// subscription managers. Endpoints are named; each manager's url func gives them its scheme.
//
// The held-off rows pin the rate-limit narrowing inside a tier: a held-off endpoint is
// skipped, on the optimizer path and the first-available path alike, while another endpoint
// of the tier is ready; when none is, the full tier serves rather than the backup tier or an
// error.
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
		sticky string
		// heldOff are the endpoints on a rate-limit hold-off.
		heldOff []string
		// stalled and off are the endpoints the router has out of rotation: held head-stalled
		// (lend rank 1) and switched off after repeated failures (lend rank 2).
		stalled []string
		off     []string
		// pick, when set, is the optimizer: it gets the tier's endpoint names and returns
		// the chosen ones. Unset means no optimizer.
		pick func(tier []string) []string

		want string // empty means selectEndpoint must fail, with the manager's exhausted error
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
		{name: "primary empty: first backup", backup: []string{"backup-1", "backup-2"}, want: "backup-1"},
		{name: "both empty: error"},

		{name: "sticky on primary wins over first-available", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, sticky: "primary-2", want: "primary-2", wantStickyAfter: "primary-2"},
		{name: "sticky on backup resolves across tiers", primary: []string{"primary-1"}, backup: []string{"backup-1"}, sticky: "backup-1", want: "backup-1", wantStickyAfter: "backup-1"},
		{name: "sticky on an unknown endpoint is kept and skipped", primary: []string{"primary-1"}, sticky: "gone-1", want: "primary-1", wantStickyAfter: "gone-1"},

		{name: "optimizer picks within primary, backup never consulted", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1", "backup-2"}, pick: pickIfPresent("primary-2"), want: "primary-2", wantOptimizerCalls: 1},
		{name: "optimizer empty over primary falls back to first primary", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1", "backup-2"}, pick: pickIfPresent("backup-2"), want: "primary-1", wantOptimizerCalls: 1},
		{name: "optimizer over backup when primary is empty", backup: []string{"backup-1", "backup-2"}, pick: pickIfPresent("backup-2"), want: "backup-2", wantOptimizerCalls: 1},
		{name: "single-endpoint tier skips the optimizer", primary: []string{"primary-1"}, pick: pickIfPresent("primary-1"), want: "primary-1"},

		{name: "held-off primary skipped: next primary, not backup", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, heldOff: []string{"primary-1"}, want: "primary-2"},
		{name: "every primary held off: full primary tier, not backup", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, heldOff: []string{"primary-1", "primary-2"}, want: "primary-1"},
		{name: "held-off backup skipped when primary is empty", backup: []string{"backup-1", "backup-2"}, heldOff: []string{"backup-1"}, want: "backup-2"},
		{name: "held-off endpoint is not offered to the optimizer", primary: []string{"primary-1", "primary-2", "primary-3"}, heldOff: []string{"primary-1"}, pick: pickIfPresent("primary-1"), want: "primary-2", wantOptimizerCalls: 1},
		{name: "optimizer picks among the ready endpoints", primary: []string{"primary-1", "primary-2", "primary-3"}, heldOff: []string{"primary-1"}, pick: pickIfPresent("primary-3"), want: "primary-3", wantOptimizerCalls: 1},
		{name: "every endpoint held off: optimizer is offered the full tier", primary: []string{"primary-1", "primary-2", "primary-3"}, backup: []string{"backup-1"}, heldOff: []string{"primary-1", "primary-2", "primary-3"}, pick: pickIfPresent("primary-3"), want: "primary-3", wantOptimizerCalls: 1},
		{name: "one ready endpoint left: single-endpoint path, optimizer skipped", primary: []string{"primary-1", "primary-2"}, heldOff: []string{"primary-1"}, pick: pickIfPresent("primary-1"), want: "primary-2"},
		{name: "single-endpoint tier held off: still serves", primary: []string{"primary-1"}, backup: []string{"backup-1"}, heldOff: []string{"primary-1"}, pick: pickIfPresent("backup-1"), want: "primary-1"},

		// MAG-4161: a url out of rotation gets no new subscription while one in rotation is left.
		{name: "off primary is skipped: next primary", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, off: []string{"primary-1"}, want: "primary-2"},
		{name: "stalled primary is skipped: next primary", primary: []string{"primary-1", "primary-2"}, stalled: []string{"primary-1"}, want: "primary-2"},
		{name: "every primary out: backup, not a primary out of rotation", primary: []string{"primary-1", "primary-2"}, backup: []string{"backup-1"}, off: []string{"primary-1"}, stalled: []string{"primary-2"}, want: "backup-1"},
		{name: "off backup is skipped: next backup", backup: []string{"backup-1", "backup-2"}, off: []string{"backup-1"}, want: "backup-2"},
		{name: "off sticky is skipped and keeps its affinity", primary: []string{"primary-1", "primary-2"}, sticky: "primary-1", off: []string{"primary-1"}, want: "primary-2", wantStickyAfter: "primary-1"},
		{name: "off sticky on backup is skipped: primary", primary: []string{"primary-1"}, backup: []string{"backup-1"}, sticky: "backup-1", off: []string{"backup-1"}, want: "primary-1", wantStickyAfter: "backup-1"},
		{name: "sticky in rotation wins while another url is off", primary: []string{"primary-1", "primary-2"}, sticky: "primary-2", off: []string{"primary-1"}, want: "primary-2", wantStickyAfter: "primary-2"},
		{name: "optimizer is never offered an off url", primary: []string{"primary-1", "primary-2", "primary-3"}, off: []string{"primary-2"}, pick: pickIfPresent("primary-2"), want: "primary-1", wantOptimizerCalls: 1},

		// Last chance: with nothing in rotation left, a url out of rotation beats no
		// subscription, in the order relay selection lends one.
		{name: "everything off: last chance takes the first primary", primary: []string{"primary-1"}, backup: []string{"backup-1"}, off: []string{"primary-1", "backup-1"}, want: "primary-1"},
		{name: "primary empty, every backup off: last chance takes the first backup", backup: []string{"backup-1", "backup-2"}, off: []string{"backup-1", "backup-2"}, want: "backup-1"},
		{name: "last chance: a stalled url before a switched-off one", primary: []string{"primary-1", "primary-2"}, off: []string{"primary-1"}, stalled: []string{"primary-2"}, want: "primary-2"},
		{name: "last chance: the primaries before the backups", primary: []string{"primary-1"}, backup: []string{"backup-1"}, off: []string{"primary-1"}, stalled: []string{"backup-1"}, want: "primary-1"},
		{name: "everything off: sticky stays where it is", primary: []string{"primary-1"}, backup: []string{"backup-1"}, sticky: "backup-1", off: []string{"primary-1", "backup-1"}, want: "backup-1", wantStickyAfter: "backup-1"},
		{name: "everything out: off sticky gives way to a stalled url", primary: []string{"primary-1"}, backup: []string{"backup-1"}, sticky: "backup-1", stalled: []string{"primary-1"}, off: []string{"backup-1"}, want: "primary-1", wantStickyAfter: "backup-1"},
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
						optimizer = &fakeSubscriptionOptimizer{chooseFn: func(addresses []string) []string {
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

					// Before build: the gRPC manager takes its registry at construction.
					reg := withFreshRelayHoldoff(t)
					for _, name := range sc.heldOff {
						reg.RecordRateLimit(manager.url(name), manager.url(name), time.Minute)
					}

					cascade := manager.build(nodeURLs(sc.primary), backup, optimizer)
					if sc.stalled != nil || sc.off != nil {
						rankOf := map[string]int{}
						for _, name := range sc.stalled {
							rankOf[manager.url(name)] = 1
						}
						for _, name := range sc.off {
							rankOf[manager.url(name)] = 2
						}
						cascade.setURLLendRank(func(url string) int { return rankOf[url] })
					}
					if sc.sticky != "" {
						cascade.stickyStore.Set("client-1", &routersession.StickySession{Provider: manager.url(sc.sticky)})
					}

					ep, err := cascade.selectEndpoint(context.Background(), "client-1")
					if sc.want == "" {
						require.EqualError(t, err, manager.exhausted)
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
