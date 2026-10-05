package relaycore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MAG-3923: the backup-reserve hedge must leave a backup one full window before the budget ends,
// must never cut the first primary's window short, and must not be armed when no reserve fits.
func TestBackupReserveDelay(t *testing.T) {
	cases := []struct {
		name           string
		budget, window time.Duration
		wantDelay      time.Duration
		wantArmed      bool
	}{
		// The production shape from the ticket: 30s budget, 10s window. Without the reserve the
		// backup's turn is the 4th window, at 30s — the moment the budget ends.
		{"production 30s/10s leaves the backup the last window", 30 * time.Second, 10 * time.Second, 20 * time.Second, true},
		{"gk8 30s/7s", 30 * time.Second, 7 * time.Second, 23 * time.Second, true},
		{"router default 30s/1s", 30 * time.Second, time.Second, 29 * time.Second, true},
		// Budget under two windows: the first primary still keeps its whole window, and the
		// backup gets what remains.
		{"short budget keeps the first window", 15 * time.Second, 10 * time.Second, 10 * time.Second, true},
		{"window equals budget: nothing to reserve", 10 * time.Second, 10 * time.Second, 0, false},
		{"window beyond budget: nothing to reserve", 10 * time.Second, 20 * time.Second, 0, false},
		{"no window", 30 * time.Second, 0, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			delay, armed := backupReserveDelay(tc.budget, tc.window)
			require.Equal(t, tc.wantArmed, armed)
			require.Equal(t, tc.wantDelay, delay)
		})
	}
}
