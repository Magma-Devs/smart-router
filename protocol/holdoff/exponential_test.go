package holdoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestExponentialFor_Ladder pins the strike ladder: the first strike (and anything below it)
// is InitialHoldoff, each further strike doubles it, capped at MaxHoldoff.
func TestExponentialFor_Ladder(t *testing.T) {
	cases := []struct {
		strikes int
		want    time.Duration
	}{
		{-1, InitialHoldoff},
		{0, InitialHoldoff},
		{1, InitialHoldoff},
		{2, time.Minute},
		{3, 2 * time.Minute},
		{6, 16 * time.Minute},
		{7, MaxHoldoff},
		{1000, MaxHoldoff},
	}
	for _, c := range cases {
		require.Equal(t, c.want, exponentialFor(c.strikes), "strikes=%d", c.strikes)
	}
}
