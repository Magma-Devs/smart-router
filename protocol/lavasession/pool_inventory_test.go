package lavasession

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// The pool-empty reason exists to separate causes that are investigated in different places. A test
// per branch, because collapsing any two of them back together is exactly the regression that makes
// the line worthless again.
func TestPoolEmptyReason_NamesTheCauseNotTheSymptom(t *testing.T) {
	for _, tt := range []struct {
		name        string
		pairingSize int
		validCount  int
		blocked     int
		addon       string
		extensions  []string
		want        string
	}{
		{
			// The zxb5l case: nothing was ever registered. Not a routing problem, and the block
			// reasons have nothing to say about it — there is no provider to have blocked.
			name: "nothing registered", pairingSize: 0, validCount: 0, blocked: 0, want: "pairing-empty",
		},
		{
			// pairing-empty wins even when a stale blocked count is present: with no pairing there is
			// nothing for the reset to restore, which is the actionable fact.
			name:        "nothing registered outranks a stale blocked count",
			pairingSize: 0, validCount: 0, blocked: 3, want: "pairing-empty",
		},
		{
			name:        "pairing present and every member blocked",
			pairingSize: 2, validCount: 0, blocked: 2, want: "all-blocked",
		},
		{
			// The regression this ordering exists to prevent. Four of the five members are unblocked
			// and simply do not serve the addon; testing blockedCount first called that all-blocked,
			// which sends the reader to the block reasons for four providers that were never blocked.
			name:        "one blocked member does not make a filtered pool all-blocked",
			pairingSize: 5, validCount: 4, blocked: 1, addon: "archive", want: "addon-filtered",
		},
		{
			name:        "addon filtered out the whole pairing",
			pairingSize: 3, validCount: 3, blocked: 0, addon: "debug", want: "addon-filtered",
		},
		{
			name:        "extension filtered out the whole pairing",
			pairingSize: 3, validCount: 3, blocked: 0, extensions: []string{"archive"}, want: "addon-filtered",
		},
		{
			// Members survived into validAddresses and the request wants the default collection,
			// which nothing filters except a provider having no usable endpoint. Registration
			// succeeded; the endpoints behind it did not.
			name:        "registered members with no usable endpoint",
			pairingSize: 3, validCount: 3, blocked: 0, want: "no-usable-endpoints",
		},
		{
			// Members registered, not all blocked, and none reached validAddresses. Not a shape we
			// model — it must not borrow one of the names above.
			name:        "unmodelled shape is named, not mislabelled",
			pairingSize: 5, validCount: 0, blocked: 1, want: "unspecified",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inv := poolInventory{pairingSize: tt.pairingSize, validCount: tt.validCount, blockedCount: tt.blocked}
			if got := inv.reason(tt.addon, tt.extensions); got != tt.want {
				t.Fatalf("poolInventory{pairing:%d valid:%d blocked:%d}.reason(%q, %v) = %q, want %q",
					tt.pairingSize, tt.validCount, tt.blocked, tt.addon, tt.extensions, got, tt.want)
			}
		})
	}
}

// The reason must be read off the REAL manager state, not only off hand-fed integers — a snapshot
// that reads the wrong field would pass every table case above and still mislabel production.
func TestSnapshotPoolInventory_ReadsTheRealState(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	full := csm.snapshotPoolInventory()
	require.Equal(t, numberOfProviders, full.pairingSize)
	require.Equal(t, numberOfProviders, full.validCount)
	require.Zero(t, full.blockedCount)
	require.False(t, full.lastPairingUpdate.IsZero(), "UpdateAllProviders must stamp the pairing time")

	// Block every member: the pool is empty for a routing reason, and the reset can undo it.
	for _, address := range append([]string{}, csm.validAddresses...) {
		require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	}
	blocked := csm.snapshotPoolInventory()
	require.Equal(t, "all-blocked", blocked.reason("", nil))

	// An empty pairing is the other cause entirely, and the reset cannot undo it.
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight+1, nil, nil))
	require.Equal(t, "pairing-empty", csm.snapshotPoolInventory().reason("", nil))
}

// An empty pairing is declined by releaseCouldServeThisRequest — its loop over pairingAddresses
// never executes — so the diagnosis has to be emitted on the guard's side of that branch. Logging
// it after the guard put the one line that explains this state behind the return that swallows it.
func TestReleaseBlockedProvidersIfPoolEmpty_ReportsAnEmptyPairing(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, nil, nil))

	require.False(t,
		csm.releaseCouldServeThisRequest(map[string]struct{}{}, "", nil, context.Background()),
		"an empty pairing must decline the release — this is the branch the report has to survive")

	records := captureLogs(t, func() {
		sessions, ok := csm.releaseBlockedProvidersIfPoolEmpty(context.Background(), 1,
			&ignoredProviders{providers: map[string]struct{}{}}, 10, spectypes.NOT_APPLICABLE,
			"", nil, 0, 0, "", "", 0, 0)
		require.Nil(t, sessions)
		require.False(t, ok, "nothing can be released from an empty pairing")
	})

	line := findLogRecord(records, "provider pool empty")
	require.NotNil(t, line, "an empty pairing must report why the pool is empty, not fall silent")
	require.Equal(t, "pairing-empty", line["reason"],
		"the empty pairing must be named as such, never as retry exhaustion")

	require.Nil(t, findLogRecord(records, "every provider has already been tried by this request, leaving the blocked list standing"),
		"no provider was tried, because there are none — that line would be false here")
}

// A pool emptied by blocking is the opposite case: the release CAN rescue it, and the reset that
// follows must be reported as the recovery it is.
func TestReleaseBlockedProvidersIfPoolEmpty_ReportsARecoveredPool(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	for _, address := range append([]string{}, csm.validAddresses...) {
		require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	}

	records := captureLogs(t, func() {
		csm.releaseBlockedProvidersIfPoolEmpty(context.Background(), 1,
			&ignoredProviders{providers: map[string]struct{}{}}, 10, spectypes.NOT_APPLICABLE,
			"", nil, 0, 0, "", "", 0, 0)
	})

	empty := findLogRecord(records, "provider pool empty")
	require.NotNil(t, empty)
	require.Equal(t, "all-blocked", empty["reason"], "blocking is a routing cause, not a registration one")

	require.NotNil(t, findLogRecord(records, "pool reset restored providers"),
		"releasing the blocked list restores the pairing, and that is a recovery")
	require.Nil(t, findLogRecord(records, "pool reset recovered no providers"))
}

// The line that blamed an expired subscription is gone. The smart router has no subscription and no
// on-chain pairing, so no empty pool can ever be caused by one — and the line sent every reader of a
// real customer capture to look at billing.
func TestResetValidAddresses_DoesNotBlameASubscription(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, nil, nil))

	records := captureLogs(t, func() { csm.resetValidAddresses("", nil) })

	for _, record := range records {
		require.NotContains(t, record["message"], "subscription",
			"an empty pool must never be reported as a subscription problem")
	}
}

// The whole value of the pairing line is `removed` — a provider silently dropping out between
// epochs is the failure it exists to catch.
func TestDiffAddressSets_ReportsWhatChanged(t *testing.T) {
	added, removed, carried := diffAddressSets(
		[]string{"tatum", "lava", "blockdaemon"},
		[]string{"lava", "blockdaemon", "chainstack"},
	)
	assertAddresses(t, "added", added, []string{"chainstack"})
	assertAddresses(t, "removed", removed, []string{"tatum"})
	assertAddresses(t, "carried_over", carried, []string{"blockdaemon", "lava"})
}

// Inputs come from Go map iteration, which is deliberately randomised. Unsorted output would read as
// churn on every epoch even when membership never moved.
func TestDiffAddressSets_OutputIsSortedRegardlessOfInputOrder(t *testing.T) {
	first, _, firstCarried := diffAddressSets([]string{"b", "a"}, []string{"a", "b", "z", "c"})
	second, _, secondCarried := diffAddressSets([]string{"a", "b"}, []string{"c", "z", "b", "a"})
	assertAddresses(t, "added (first order)", first, []string{"c", "z"})
	assertAddresses(t, "added (second order)", second, []string{"c", "z"})
	assertAddresses(t, "carried (first order)", firstCarried, []string{"a", "b"})
	assertAddresses(t, "carried (second order)", secondCarried, []string{"a", "b"})
}

// A nil slice renders as "" in the log line, which reads as a missing field rather than as
// "nothing changed" — the two must not look alike on the one line that reports pool churn.
func TestDiffAddressSets_UnchangedMembershipRendersEmptyNotNil(t *testing.T) {
	added, removed, carried := diffAddressSets([]string{"lava"}, []string{"lava"})
	if added == nil || removed == nil {
		t.Fatalf("added/removed must be non-nil when nothing changed, got added=%#v removed=%#v", added, removed)
	}
	assertAddresses(t, "added", added, []string{})
	assertAddresses(t, "removed", removed, []string{})
	assertAddresses(t, "carried_over", carried, []string{"lava"})
}

// "Never updated" and "updated at the zero time" are different states; only one of them is real.
func TestFormatPairingUpdateTime_ZeroMeansNever(t *testing.T) {
	if got := formatPairingUpdateTime(time.Time{}); got != "never" {
		t.Fatalf("zero time rendered %q, want \"never\"", got)
	}
	at := time.Date(2026, 8, 27, 19, 58, 2, 0, time.UTC)
	if got := formatPairingUpdateTime(at); got != "2026-08-27T19:58:02Z" {
		t.Fatalf("rendered %q, want RFC3339 in UTC", got)
	}
}

func assertAddresses(t *testing.T, field string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", field, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", field, got, want)
		}
	}
}

// captureLogs records everything logged during fn as parsed JSON. It drives the real sink rather
// than a stub, so a test asserting on a log line fails when the line stops being emitted — which is
// the only failure mode that matters for a change whose entire product is log output.
func captureLogs(t *testing.T, fn func()) []map[string]any {
	t.Helper()
	utils.EnableDebugLogBuffer(1000)
	utils.ClearDebugLogBuffer()

	fn()

	raw := utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 1000)
	records := make([]map[string]any, 0, len(raw))
	for _, line := range raw {
		record := map[string]any{}
		if err := json.Unmarshal(line, &record); err != nil {
			continue // a record that is not JSON cannot be the structured line under test
		}
		records = append(records, record)
	}
	return records
}

func findLogRecord(records []map[string]any, message string) map[string]any {
	for _, record := range records {
		if record["message"] == message {
			return record
		}
	}
	return nil
}
