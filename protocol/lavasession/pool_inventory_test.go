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
		name           string
		pairingSize    int
		capable        int
		capableBlocked int
		addon          string
		extensions     []string
		want           string
	}{
		{
			// The zxb5l case: nothing was ever registered. Not a routing problem, and the block
			// reasons have nothing to say about it — there is no provider to have blocked.
			name: "nothing registered", pairingSize: 0, want: "pairing-empty",
		},
		{
			// pairing-empty wins even when a stale blocked count is present: with no pairing there is
			// nothing for the reset to restore, which is the actionable fact.
			name:        "nothing registered outranks a stale blocked count",
			pairingSize: 0, capable: 0, capableBlocked: 3, want: "pairing-empty",
		},
		{
			name:        "pairing present and every member that could serve is blocked",
			pairingSize: 2, capable: 2, capableBlocked: 2, want: "all-blocked",
		},
		{
			// The mislabel this line exists to prevent, in its first form: four of five members are
			// unblocked and simply do not serve the addon. Keying on blockedCount called that
			// all-blocked, sending the reader to block reasons for providers never blocked. Only one
			// member is addon-capable and it is blocked, so the honest answer is all-blocked — for
			// the ONE provider that matters, not the four that were never candidates.
			name:        "one capable member, and it is blocked",
			pairingSize: 5, capable: 1, capableBlocked: 1, addon: "archive", want: "all-blocked",
		},
		{
			// The same mislabel in its mirror form. Keying on validCount called this addon-filtered
			// because four unrelated members were still valid, when the truth is that the two
			// providers that serve the addon are both blocked.
			name:        "capable members all blocked while unrelated members stay valid",
			pairingSize: 6, capable: 2, capableBlocked: 2, extensions: []string{"archive"}, want: "all-blocked",
		},
		{
			// Genuinely filtered: not one member serves the addon. A spec or config problem, and no
			// amount of routing recovers it.
			name:        "no member serves the addon at all",
			pairingSize: 3, capable: 0, addon: "debug", want: "addon-filtered",
		},
		{
			name:        "no member serves the extension at all",
			pairingSize: 3, capable: 0, extensions: []string{"archive"}, want: "addon-filtered",
		},
		{
			// The default collection is served by every provider by definition, so the only predicate
			// left is having at least one endpoint. Nothing capable means they registered with none.
			name:        "registered members with no endpoints at all",
			pairingSize: 3, capable: 0, want: "no-usable-endpoints",
		},
		{
			// Capable members exist and are not all blocked, yet nothing was selectable. Not a shape
			// we model — it must not borrow one of the names above.
			name:        "unmodelled shape is named, not mislabelled",
			pairingSize: 5, capable: 3, capableBlocked: 1, want: "unspecified",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inv := poolInventory{pairingSize: tt.pairingSize, capable: tt.capable, capableBlocked: tt.capableBlocked}
			if got := inv.reason(tt.addon, tt.extensions); got != tt.want {
				t.Fatalf("poolInventory{pairing:%d capable:%d capableBlocked:%d}.reason(%q, %v) = %q, want %q",
					tt.pairingSize, tt.capable, tt.capableBlocked, tt.addon, tt.extensions, got, tt.want)
			}
		})
	}
}

// The reason must be read off the REAL manager state, not only off hand-fed integers — a snapshot
// that reads the wrong field would pass every table case above and still mislabel production.
func TestSnapshotPoolInventory_ReadsTheRealState(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	full := csm.snapshotPoolInventory("", nil, context.Background())
	require.Equal(t, numberOfProviders, full.pairingSize)
	require.Equal(t, numberOfProviders, full.validCount)
	require.Zero(t, full.blockedCount)
	require.False(t, full.lastPairingUpdate.IsZero(), "UpdateAllProviders must stamp the pairing time")

	// Block every member: the pool is empty for a routing reason, and the reset can undo it.
	for _, address := range append([]string{}, csm.validAddresses...) {
		require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	}
	blocked := csm.snapshotPoolInventory("", nil, context.Background())
	require.Equal(t, "all-blocked", blocked.reason("", nil))

	// An empty pairing is the other cause entirely, and the reset cannot undo it.
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight+1, nil, nil))
	require.Equal(t, "pairing-empty", csm.snapshotPoolInventory("", nil, context.Background()).reason("", nil))
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

	// UpdateAllProviders is inside the capture on purpose. Resetting an empty pairing logs nothing
	// at all now, so capturing only that leaves records empty and the assertion below iterates zero
	// times — green whether or not the sink works, and green whether or not the line came back.
	// Including the pairing update guarantees at least one record, which makes NotEmpty a real
	// check that the sink is capturing and the loop a real check of what it captured.
	records := captureLogs(t, func() {
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, nil, nil))
		csm.resetValidAddresses("", nil)
	})

	require.NotEmpty(t, records,
		"the sink captured nothing — the assertion below would pass vacuously")
	for _, record := range records {
		require.NotContains(t, logMessage(record), "subscription",
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

// Unchanged membership returns empty slices rather than nil. The log cannot tell the two apart —
// both render as "" — so this is a contract for the callers: a result is always safe to range over
// and append to without a nil check.
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
// debugRingCapacity is deliberately far above what these tests emit. The ring evicts oldest-first,
// and the record every assertion here looks for — "provider pool empty" — is the FIRST one emitted
// on the path. Size it to the run and any added logging silently evicts the line under test, turning
// these into red tests that claim a log line was never emitted when it was emitted and dropped.
const debugRingCapacity = 100000

// logMessage reads a record's message without assuming it has one. A record reaching the shared ring
// without a "message" key — a line sent via .Send() rather than .Msg() — would otherwise fail
// require.NotContains on a nil argument, with an error about builtin len() that points nowhere near
// the actual subject.
func logMessage(record map[string]any) string {
	message, _ := record["message"].(string)
	return message
}

func captureLogs(t *testing.T, fn func()) []map[string]any {
	t.Helper()
	// EnableDebugLogBuffer swaps a process-global sink. Without the cleanup every later test in this
	// binary keeps paying the copy-and-append on every log call, and the sink's documented
	// "disabled by default" contract stays broken for the rest of the run.
	utils.EnableDebugLogBuffer(debugRingCapacity)
	t.Cleanup(utils.DisableDebugLogBuffer)
	utils.ClearDebugLogBuffer()

	fn()

	raw := utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, debugRingCapacity)
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

// The guard declines the release for every structural emptiness, not only an empty pairing: a
// pairing where nothing serves the requested addon reaches the same return. Keying the diagnosis on
// pairingSize rescued only the empty-pairing case and left this one reporting "every provider has
// already been tried" — for a request that tried nothing at all.
func TestReleaseBlockedProvidersIfPoolEmpty_ReportsAnAddonNothingServes(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	records := captureLogs(t, func() {
		_, ok := csm.releaseBlockedProvidersIfPoolEmpty(context.Background(), 1,
			&ignoredProviders{providers: map[string]struct{}{}}, 10, spectypes.NOT_APPLICABLE,
			"nonexistent-addon", nil, 0, 0, "", "", 0, 0)
		require.False(t, ok, "no provider serves this addon, so nothing can be released")
	})

	line := findLogRecord(records, "provider pool empty")
	require.NotNil(t, line, "a pairing that cannot serve the addon must say so, not fall silent")
	require.Equal(t, "addon-filtered", line["reason"],
		"no member serves this addon — that is a config problem, not retry exhaustion")

	require.Nil(t, findLogRecord(records, "every provider has already been tried by this request, leaving the blocked list standing"),
		"nothing was tried: the ignored set was empty on entry")
}

// An empty pool is a property of the pairing, and the pairing does not change between relays. A
// chain whose providers all failed startup verification stays empty indefinitely, and this path runs
// on every relay — reporting it per-relay at WARN turns a permanent state into a sustained alert
// stream, which is the log flood this work exists to reduce.
func TestReleaseBlockedProvidersIfPoolEmpty_WarnsOncePerPairing(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, nil, nil))

	callOnce := func() map[string]any {
		var line map[string]any
		records := captureLogs(t, func() {
			csm.releaseBlockedProvidersIfPoolEmpty(context.Background(), 1,
				&ignoredProviders{providers: map[string]struct{}{}}, 10, spectypes.NOT_APPLICABLE,
				"", nil, 0, 0, "", "", 0, 0)
		})
		line = findLogRecord(records, "provider pool empty")
		require.NotNil(t, line, "the state must still be reported on every pass, at some level")
		return line
	}

	first := callOnce()
	require.Equal(t, "warn", first["level"], "the first report for a pairing is the alertable one")
	require.Equal(t, "false", first["repeat"])

	second := callOnce()
	require.Equal(t, "debug", second["level"],
		"a repeat for the same pairing must not warn again — the state has not changed")
	require.Equal(t, "true", second["repeat"])

	// A genuinely new pairing is a new outage, and must warn immediately rather than inherit the
	// throttle from the pairing that preceded it.
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight+1, nil, nil))
	afterRebuild := callOnce()
	require.Equal(t, "warn", afterRebuild["level"],
		"a rebuilt pairing that is still empty is a fresh event, not a repeat")
}

// The pairing inventory is the answer to "why was the primary not even a candidate?", and `removed`
// is the field that carries it: a provider that silently leaves the pool between epochs is invisible
// from the selection path, where every line reports what IS in the pool and none reports what left.
//
// This drives UpdateAllProviders through the real log sink rather than testing diffAddressSets in
// isolation, for the same reason the pool-empty tests do: a helper test cannot tell you whether the
// line is ever reached, and the line is the entire product of the change.
func TestUpdateAllProviders_ReportsPairingMembershipAndChurn(t *testing.T) {
	csm := CreateConsumerSessionManager()

	firstPush := captureLogs(t, func() {
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
			createNamedPairingList("gamma", "alpha", "beta"), nil))
	})

	line := findLogRecord(firstPush, "pairing updated")
	require.NotNil(t, line, "every provider-set push must report the resulting membership")
	require.Equal(t, "3", line["size"])
	require.Equal(t, "alpha,beta,gamma", line["providers"],
		"membership is sorted: the source is Go map iteration, and an unsorted line reads as churn")
	require.Equal(t, "alpha,beta,gamma", line["added"], "the first push adds everything")
	require.Equal(t, "", line["removed"])

	// The push that matters. A provider dropping out between epochs is the failure this line exists
	// to make visible, and `removed` is the only place it appears.
	secondPush := captureLogs(t, func() {
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight+1,
			createNamedPairingList("alpha", "beta"), nil))
	})

	line = findLogRecord(secondPush, "pairing updated")
	require.NotNil(t, line)
	require.Equal(t, "gamma", line["removed"],
		"a provider that left the pairing must be named — nothing else in the router reports it")
	require.Equal(t, "alpha,beta", line["carried_over"])
	require.Equal(t, "", line["added"])
	require.Equal(t, "2", line["size"])
}

// The backup tier gets the same treatment for the same reason: since MAG-2525 a chain can serve on
// backups alone, so a backup leaving the pool is the same invisible failure with the same blast
// radius. Reported on its own fields so a backup change is not mistaken for a primary one.
func TestUpdateAllProviders_ReportsBackupPoolChurnSeparately(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		createNamedPairingList("alpha"), createNamedPairingList("backup-one", "backup-two")))

	records := captureLogs(t, func() {
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight+1,
			createNamedPairingList("alpha"), createNamedPairingList("backup-one")))
	})

	line := findLogRecord(records, "pairing updated")
	require.NotNil(t, line)
	require.Equal(t, "backup-two", line["backup_removed"],
		"a backup leaving the pool is reported, and on the backup fields")
	require.Equal(t, "1", line["backup_pool_size"])
	require.Equal(t, "backup-one", line["backup_providers"])
	// The primary tier did not move, and must not be reported as though it had.
	require.Equal(t, "", line["removed"])
	require.Equal(t, "alpha", line["carried_over"])
}
