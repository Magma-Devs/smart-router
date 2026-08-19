package endpointstate

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// specRequiresHeadOnly is the spec half of the production selector for MAG-2218's head-only
// mode: the tracker tests set the config flag directly, so without this the mode could be
// entirely dead in production and every one of them would still pass. The operator half
// (--enable-fork-detection) is covered by TestResolveHashPolling below.
func TestSpecRequiresHeadOnly_Selector(t *testing.T) {
	directive := &spectypes.ParseDirective{}

	for _, tc := range []struct {
		name string
		// nil means the tag is absent (ok=false); otherwise the directive returned with ok=true.
		latest *spectypes.ParseDirective
		byNum  *spectypes.ParseDirective
		// present marks a tag returned with ok=true, which lets us cover ok=true + nil parsing.
		latestPresent bool
		byNumPresent  bool
		want          bool
	}{
		{
			name:          "both tags present: normal chain, hashed tracking",
			latest:        directive,
			byNum:         directive,
			latestPresent: true, byNumPresent: true,
			want: false,
		},
		{
			name:          "head only, no block-by-number: the Canton shape",
			latest:        directive,
			latestPresent: true,
			want:          true,
		},
		{
			name: "neither tag: not head-only, just untrackable",
			want: false,
		},
		{
			name:         "no head but has block-by-number: not head-only",
			byNum:        directive,
			byNumPresent: true,
			want:         false,
		},
		{
			// GetParsingByTag returns the map value straight through, so a tag registered with a
			// nil directive yields ok=true and an unusable parsing. Checking ok alone would keep
			// this chain in the hash-fetching path — the failure head-only exists to avoid.
			name:          "block-by-number present but nil: treated as absent",
			latest:        directive,
			latestPresent: true, byNumPresent: true, byNum: nil,
			want: true,
		},
		{
			name:          "head present but nil: not head-only",
			latestPresent: true, latest: nil,
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			parser := chainlib.NewMockChainParser(ctrl)
			parser.EXPECT().GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM).
				Return(tc.latest, nil, tc.latestPresent).AnyTimes()
			parser.EXPECT().GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM).
				Return(tc.byNum, nil, tc.byNumPresent).AnyTimes()

			m := &EndpointMonitor{chainParser: parser}
			require.Equal(t, tc.want, m.specRequiresHeadOnly())
		})
	}
}

func TestSpecRequiresHeadOnly_NilParserIsSafe(t *testing.T) {
	m := &EndpointMonitor{}
	require.False(t, m.specRequiresHeadOnly(), "a nil parser must not panic or enable head-only")
}

// TestResolveHashPolling covers the operator flag and, critically, the PRECEDENCE between the
// two reasons. They both produce head-only, so only the reported reason distinguishes them —
// and an operator who sees "off-operator-choice" on a Canton-shaped chain would turn the flag
// on and watch nothing change.
func TestResolveHashPolling(t *testing.T) {
	directive := &spectypes.ParseDirective{}

	for _, tc := range []struct {
		name                string
		specCanHash         bool
		enableForkDetection bool
		want                HashPollingReason
		wantHeadOnly        bool
	}{
		{
			name:        "flag off, spec can hash: the new default",
			specCanHash: true, enableForkDetection: false,
			want: HashPollingOffOperatorChoice, wantHeadOnly: true,
		},
		{
			name:        "flag on, spec can hash: fork detection runs",
			specCanHash: true, enableForkDetection: true,
			want: HashPollingOn, wantHeadOnly: false,
		},
		{
			name:        "flag off, spec cannot hash: spec reason wins",
			specCanHash: false, enableForkDetection: false,
			want: HashPollingOffSpecUnsupported, wantHeadOnly: true,
		},
		{
			// The precedence case that matters: turning the flag ON must NOT claim fork
			// detection is running on a chain that physically cannot serve hashes.
			name:        "flag on, spec cannot hash: still off, and says why",
			specCanHash: false, enableForkDetection: true,
			want: HashPollingOffSpecUnsupported, wantHeadOnly: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			parser := chainlib.NewMockChainParser(ctrl)
			parser.EXPECT().GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM).
				Return(directive, nil, true).AnyTimes()
			// Absent GET_BLOCK_BY_NUM is what makes a chain unable to hash.
			byNum := directive
			if !tc.specCanHash {
				byNum = nil
			}
			parser.EXPECT().GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM).
				Return(byNum, nil, tc.specCanHash).AnyTimes()

			m := &EndpointMonitor{chainParser: parser}
			got := m.resolveHashPolling(tc.enableForkDetection)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantHeadOnly, got.HeadOnly())
		})
	}
}

// TestNewEndpointMonitor_ForkDetectionFlagReachesTrackers closes the wiring gap the two tests
// above cannot see: they call the resolver directly, so the config field could fail to reach
// it and both would still pass. This asserts the value a real NewEndpointMonitor settles on.
func TestNewEndpointMonitor_ForkDetectionFlagReachesTrackers(t *testing.T) {
	newMonitor := func(t *testing.T, enable bool) *EndpointMonitor {
		t.Helper()
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		directive := &spectypes.ParseDirective{}
		parser := chainlib.NewMockChainParser(ctrl)
		// A normal chain: both tags present, so the spec never forces head-only and the flag
		// is the only thing deciding.
		parser.EXPECT().GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM).
			Return(directive, nil, true).AnyTimes()
		parser.EXPECT().GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM).
			Return(directive, nil, true).AnyTimes()

		m := NewEndpointMonitor(context.Background(), EndpointChainTrackerConfig{
			ChainParser:         parser,
			ChainID:             "ETH1",
			ApiInterface:        "jsonrpc",
			AverageBlockTime:    time.Second,
			EnableForkDetection: enable,
		})
		t.Cleanup(m.Stop)
		return m
	}

	t.Run("default (flag off): hash polling is off by operator choice", func(t *testing.T) {
		m := newMonitor(t, false)
		require.Equal(t, HashPollingOffOperatorChoice, m.HashPollingMode())
		require.True(t, m.HashPollingMode().HeadOnly(), "trackers must be built head-only")
	})

	t.Run("flag on: hash polling runs", func(t *testing.T) {
		m := newMonitor(t, true)
		require.Equal(t, HashPollingOn, m.HashPollingMode())
		require.False(t, m.HashPollingMode().HeadOnly(), "trackers must do fork detection")
	})
}
