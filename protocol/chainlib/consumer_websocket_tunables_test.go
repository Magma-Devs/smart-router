package chainlib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWebSocketKeepAliveOutlivesIdleReaper pins when the startup warning fires: only
// when pings are on and the idle limit is off. Either feature alone changes nothing
// about how long a connection can live.
func TestWebSocketKeepAliveOutlivesIdleReaper(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keepAlive time.Duration
		idle      int64
		want      bool
	}{
		{"defaults: pinged, reaped at 20 minutes", DefaultWebSocketKeepAliveInterval, DefaultMaxIdleTimeInSeconds, false},
		{"pinged, never reaped", DefaultWebSocketKeepAliveInterval, 0, true},
		{"pinged, negative idle limit counts as off", DefaultWebSocketKeepAliveInterval, -1, true},
		{"no pings, never reaped: the proxy reaps it", 0, 0, false},
		{"no pings, reaped", 0, DefaultMaxIdleTimeInSeconds, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, WebSocketKeepAliveOutlivesIdleReaper(tc.keepAlive, tc.idle))
		})
	}
}
