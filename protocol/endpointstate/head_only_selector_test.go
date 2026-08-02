package endpointstate

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// headOnlyTracking is the production selector for MAG-2218's head-only mode: the tracker
// tests set the config flag directly, so without this the mode could be entirely dead in
// production and every one of them would still pass.
func TestHeadOnlyTracking_Selector(t *testing.T) {
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
			require.Equal(t, tc.want, m.headOnlyTracking())
		})
	}
}

func TestHeadOnlyTracking_NilParserIsSafe(t *testing.T) {
	m := &EndpointMonitor{}
	require.False(t, m.headOnlyTracking(), "a nil parser must not panic or enable head-only")
}
