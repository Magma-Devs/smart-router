package relaycore

import (
	"crypto/sha256"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// hashOf gives each candidate a distinct, stable [32]byte key so winner identity can be asserted.
func hashOf(label string) [32]byte { return sha256.Sum256([]byte(label)) }

// entry builds a resultCount whose total count is the sum of its per-group tallies, mirroring how the
// scan loop in responsesCrossValidation accumulates a hash's agreement.
func entry(label string, groupCounts map[string]int) *resultCount {
	total := 0
	for _, c := range groupCounts {
		total += c
	}
	return &resultCount{
		count:       total,
		result:      common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(label)}, ProviderInfo: common.ProviderInfo{ProviderAddress: "p-" + label}},
		groupCounts: groupCounts,
	}
}

// TestSelectQuorumWinner_MinGroups covers the default (MinGroups) selection mode: a winner must meet BOTH
// the agreement threshold and the distinct-group requirement, a higher-count-but-non-diverse hash must not
// shadow a diverse one (P1), and the maxRealCount / maxCount split that drives the failure-reason labels.
// Every case keeps the deciding count strictly unique so the (order-dependent) tie-break is never exercised.
func TestSelectQuorumWinner_MinGroups(t *testing.T) {
	cases := []struct {
		name           string
		countMap       map[[32]byte]*resultCount
		threshold      int
		minGroups      int
		wantFound      bool
		wantWinner     string // result data of the expected winner (when found)
		wantCount      int
		wantDistinct   int
		wantMaxCount   int
		wantMaxRealCnt int
	}{
		{
			name:      "single hash meets threshold and diversity -> winner",
			countMap:  map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 1, "g2": 1})},
			threshold: 2, minGroups: 2,
			wantFound: true, wantWinner: "A", wantCount: 2, wantDistinct: 2, wantMaxCount: 2, wantMaxRealCnt: 2,
		},
		{
			// P1: B has the higher count (5) but only one group; A meets both with count 2. A must win.
			name: "higher-count non-diverse hash must not shadow a diverse one",
			countMap: map[[32]byte]*resultCount{
				hashOf("A"): entry("A", map[string]int{"g1": 1, "g2": 1}),
				hashOf("B"): entry("B", map[string]int{"g1": 5}),
			},
			threshold: 2, minGroups: 2,
			wantFound: true, wantWinner: "A", wantCount: 2, wantDistinct: 2, wantMaxCount: 5, wantMaxRealCnt: 5,
		},
		{
			name:      "no hash reaches the threshold -> no winner (no-agreement: maxRealCount < threshold)",
			countMap:  map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 1})},
			threshold: 3, minGroups: 1,
			wantFound: false, wantMaxCount: 1, wantMaxRealCnt: 1,
		},
		{
			name:      "threshold met but diversity unmet -> no winner (diversity-unmet: maxRealCount >= threshold)",
			countMap:  map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 3})},
			threshold: 3, minGroups: 2,
			wantFound: false, wantMaxCount: 3, wantMaxRealCnt: 3,
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := selectQuorumWinner(0, tc.countMap, nil, 0, -1, nil, tc.threshold, tc.minGroups, false)
			require.Equal(t, tc.wantFound, w.found, "found, tc #%d, i #%d", i, i)
			require.Equal(t, tc.wantMaxCount, w.maxCount, "maxCount, tc #%d, i #%d", i, i)
			require.Equal(t, tc.wantMaxRealCnt, w.maxRealCount, "maxRealCount, tc #%d, i #%d", i, i)
			if tc.wantFound {
				require.Equal(t, hashOf(tc.wantWinner), w.hash, "winner hash, tc #%d, i #%d", i, i)
				require.Equal(t, []byte(tc.wantWinner), w.result.Reply.Data, "winner result, tc #%d, i #%d", i, i)
				require.Equal(t, tc.wantCount, w.count, "count, tc #%d, i #%d", i, i)
				require.Equal(t, tc.wantDistinct, w.distinctGroups, "distinctGroups, tc #%d, i #%d", i, i)
			}
		})
	}
}

// TestSelectQuorumWinner_PerGroup covers per-group-quorum mode: a winner needs minGroups groups that EACH
// independently reach the threshold, anyGroupReachedQuorum reports whether even one group qualified, and
// nil replies are never a per-group fallback. Counts are kept unique so the qualifying-group tie-break
// resolves deterministically on count.
func TestSelectQuorumWinner_PerGroup(t *testing.T) {
	t.Run("two groups each reach the internal threshold -> winner", func(t *testing.T) {
		cm := map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 2, "g2": 2})}
		w := selectQuorumWinner(0, cm, nil, 0, -1, nil, 2, 2, true)
		require.True(t, w.found)
		require.Equal(t, "A", string(w.result.Reply.Data))
		require.Equal(t, 4, w.count)
		require.Equal(t, 2, w.qualifyingGroups)
		require.True(t, w.anyGroupReachedQuorum)
	})

	t.Run("only one group reaches its internal threshold -> no winner, but anyGroupReachedQuorum true", func(t *testing.T) {
		// g1 has 2 (>= threshold), g2 has 1 (< threshold): only one qualifying group, need two.
		cm := map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 2, "g2": 1})}
		w := selectQuorumWinner(0, cm, nil, 0, -1, nil, 2, 2, true)
		require.False(t, w.found)
		require.True(t, w.anyGroupReachedQuorum, "g1 reached its internal quorum even though the request failed")
	})

	t.Run("no group reaches the internal threshold -> anyGroupReachedQuorum false", func(t *testing.T) {
		cm := map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 1, "g2": 1})}
		w := selectQuorumWinner(0, cm, nil, 0, -1, nil, 2, 2, true)
		require.False(t, w.found)
		require.False(t, w.anyGroupReachedQuorum)
	})

	t.Run("equal qualifying groups -> tie-broken by total count (unique counts, deterministic)", func(t *testing.T) {
		// Both A and B are corroborated by 2 qualifying groups; B has the strictly higher total count.
		cm := map[[32]byte]*resultCount{
			hashOf("A"): entry("A", map[string]int{"g1": 2, "g2": 2}), // count 4
			hashOf("B"): entry("B", map[string]int{"g3": 3, "g4": 2}), // count 5
		}
		w := selectQuorumWinner(0, cm, nil, 0, -1, nil, 2, 2, true)
		require.True(t, w.found)
		require.Equal(t, hashOf("B"), w.hash, "higher-count hash wins the qualifying-groups tie")
		require.Equal(t, 5, w.count)
	})

	t.Run("per-group ignores nil replies entirely", func(t *testing.T) {
		// A plentiful, diverse nil set would win in MinGroups mode but must be ignored under per-group.
		nilRes := common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "nil"}}
		w := selectQuorumWinner(0, map[[32]byte]*resultCount{}, []common.RelayResult{nilRes}, 9, 0, map[string]struct{}{"g1": {}, "g2": {}}, 2, 2, true)
		require.False(t, w.found, "nil replies are not an independent corroboration under per-group quorum")
	})
}

// TestSelectQuorumWinner_NilFallback covers the nil-reply fallback in MinGroups mode: it wins only when no
// real hash formed a diverse quorum, the winner's hash stays the zero sentinel, and — critically —
// maxRealCount is captured BEFORE nil replies inflate maxCount (so the no-agreement vs diversity-unmet
// failure split keys off real agreement, not nils).
func TestSelectQuorumWinner_NilFallback(t *testing.T) {
	t.Run("diverse nil quorum wins when no real hash qualifies; hash stays zero", func(t *testing.T) {
		nilRes := common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "nil"}}
		w := selectQuorumWinner(0, map[[32]byte]*resultCount{}, []common.RelayResult{nilRes}, 3, 0, map[string]struct{}{"g1": {}, "g2": {}}, 2, 2, false)
		require.True(t, w.found)
		require.Equal(t, [32]byte{}, w.hash, "nil-reply winner carries the zero hash sentinel")
		require.Equal(t, 3, w.count)
		require.Equal(t, 2, w.distinctGroups)
		require.Equal(t, "nil", w.result.ProviderInfo.ProviderAddress)
	})

	t.Run("maxRealCount excludes nils so a plentiful nil count reads as no-agreement, not diversity-unmet", func(t *testing.T) {
		// One real hash with count 1 (below threshold 3), plus 5 nil replies spanning 2 groups.
		// nilReplyIdx must point at a real slot — production never reports nilReplies>0 with an unset index.
		nilRes := common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "nil"}}
		cm := map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 1})}
		w := selectQuorumWinner(0, cm, []common.RelayResult{nilRes}, 5, 0, map[string]struct{}{"g1": {}, "g2": {}}, 3, 2, false)
		require.True(t, w.found, "the diverse nil quorum (5 >= 3, 2 groups) wins")
		require.Equal(t, 1, w.maxRealCount, "maxRealCount reflects only the real hash, before nil inflation")
		require.Equal(t, 5, w.maxCount, "maxCount is inflated by the nil replies")
	})

	t.Run("a real diverse quorum is preferred over nils", func(t *testing.T) {
		// Real hash A meets threshold and diversity; nils are plentiful but must not displace it.
		cm := map[[32]byte]*resultCount{hashOf("A"): entry("A", map[string]int{"g1": 1, "g2": 1})}
		w := selectQuorumWinner(0, cm, []common.RelayResult{{ProviderInfo: common.ProviderInfo{ProviderAddress: "nil"}}}, 9, 0, map[string]struct{}{"g1": {}, "g2": {}}, 2, 2, false)
		require.True(t, w.found)
		require.Equal(t, hashOf("A"), w.hash, "a real diverse quorum wins over the nil fallback")
		require.Equal(t, 2, w.count)
	})
}
