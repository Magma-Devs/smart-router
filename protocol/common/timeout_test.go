package common

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// resetTimeoutGlobals restores the two configurable globals after each test
// so tests don't bleed into each other.
func resetTimeoutGlobals(t *testing.T, origMin, origDefault time.Duration) {
	t.Helper()
	t.Cleanup(func() {
		MinimumTimePerRelayDelay = origMin
		DefaultTimeout = origDefault
	})
}

// --- ValidateAndCapMinRelayTimeout ---

func TestValidateAndCapMinRelayTimeout(t *testing.T) {
	defaultReset := time.Duration(DefaultTimeoutSeconds) * time.Second

	tests := []struct {
		name        string
		inputMin    time.Duration
		inputDef    time.Duration
		wantMin     time.Duration
		wantDefault time.Duration
	}{
		{
			name:        "min < default: no change",
			inputMin:    5 * time.Second,
			inputDef:    30 * time.Second,
			wantMin:     5 * time.Second,
			wantDefault: 30 * time.Second,
		},
		{
			name:        "min == default: cap to 50%",
			inputMin:    30 * time.Second,
			inputDef:    30 * time.Second,
			wantMin:     15 * time.Second,
			wantDefault: 30 * time.Second,
		},
		{
			name:        "min > default: cap to 50%",
			inputMin:    60 * time.Second,
			inputDef:    30 * time.Second,
			wantMin:     15 * time.Second,
			wantDefault: 30 * time.Second,
		},
		{
			name:        "cap is exactly half of default",
			inputMin:    25 * time.Second,
			inputDef:    20 * time.Second,
			wantMin:     10 * time.Second,
			wantDefault: 20 * time.Second,
		},
		{
			name:        "zero default: resets to default constant, min unchanged",
			inputMin:    time.Second,
			inputDef:    0,
			wantMin:     time.Second,
			wantDefault: defaultReset,
		},
		{
			name:        "negative default: resets to default constant, min unchanged",
			inputMin:    time.Second,
			inputDef:    -5 * time.Second,
			wantMin:     time.Second,
			wantDefault: defaultReset,
		},
		{
			name:        "zero default then min > reset default: resets default then caps min",
			inputMin:    60 * time.Second,
			inputDef:    0,
			wantMin:     defaultReset / 2,
			wantDefault: defaultReset,
		},
		{
			name:        "very small default (1ns): resets to default constant",
			inputMin:    time.Second,
			inputDef:    1, // 1 nanosecond
			wantMin:     time.Second,
			wantDefault: defaultReset,
		},
		{
			name:        "zero min: resets to 1s",
			inputMin:    0,
			inputDef:    30 * time.Second,
			wantMin:     time.Second,
			wantDefault: 30 * time.Second,
		},
		{
			name:        "negative min: resets to 1s",
			inputMin:    -time.Second,
			inputDef:    30 * time.Second,
			wantMin:     time.Second,
			wantDefault: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetTimeoutGlobals(t, MinimumTimePerRelayDelay, DefaultTimeout)
			MinimumTimePerRelayDelay = tt.inputMin
			DefaultTimeout = tt.inputDef

			ValidateAndCapMinRelayTimeout()

			require.Equal(t, tt.wantMin, MinimumTimePerRelayDelay)
			require.Equal(t, tt.wantDefault, DefaultTimeout)
		})
	}
}

// --- ValidateAndCapMinRelayTimeout: the CacheTimeout guard ---

func TestValidateAndCapCacheTimeout(t *testing.T) {
	tests := []struct {
		name  string
		input time.Duration
		want  time.Duration
	}{
		{name: "positive value kept (WAN-sized budget)", input: 400 * time.Millisecond, want: 400 * time.Millisecond},
		{name: "default kept", input: DefaultCacheTimeout, want: DefaultCacheTimeout},
		{name: "zero resets to default", input: 0, want: DefaultCacheTimeout},
		{name: "negative resets to default", input: -time.Second, want: DefaultCacheTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := CacheTimeout
			t.Cleanup(func() { CacheTimeout = orig })
			CacheTimeout = tt.input

			ValidateAndCapMinRelayTimeout()

			require.Equal(t, tt.want, CacheTimeout)
		})
	}
}

// --- GetTimePerCu respects MinimumTimePerRelayDelay ---

func TestGetTimePerCu_DefaultFloor(t *testing.T) {
	resetTimeoutGlobals(t, MinimumTimePerRelayDelay, DefaultTimeout)

	MinimumTimePerRelayDelay = time.Second

	// cu=10 → 10×100ms = 1s == floor → returns floor
	require.Equal(t, time.Second, GetTimePerCu(10))
	// cu=5 → 500ms < floor → returns floor
	require.Equal(t, time.Second, GetTimePerCu(5))
	// cu=20 → 2s > floor → returns 2s
	require.Equal(t, 2*time.Second, GetTimePerCu(20))
}

func TestGetTimePerCu_RaisedFloor(t *testing.T) {
	resetTimeoutGlobals(t, MinimumTimePerRelayDelay, DefaultTimeout)

	MinimumTimePerRelayDelay = 5 * time.Second

	// cu=10 → 1s < new floor (5s) → returns 5s
	require.Equal(t, 5*time.Second, GetTimePerCu(10))
	// cu=20 → 2s < new floor (5s) → returns 5s
	require.Equal(t, 5*time.Second, GetTimePerCu(20))
	// cu=100 → 10s > new floor (5s) → returns 10s
	require.Equal(t, 10*time.Second, GetTimePerCu(100))
}

func TestGetTimePerCu_FloorAfterValidation(t *testing.T) {
	resetTimeoutGlobals(t, MinimumTimePerRelayDelay, DefaultTimeout)

	// Simulate: user sets --min-relay-timeout=40s --default-processing-timeout=30s
	DefaultTimeout = 30 * time.Second
	MinimumTimePerRelayDelay = 40 * time.Second
	ValidateAndCapMinRelayTimeout() // should cap to 15s

	// cu=10 → 1s < 15s → returns 15s
	require.Equal(t, 15*time.Second, GetTimePerCu(10))
	// cu=200 → 20s > 15s → returns 20s
	require.Equal(t, 20*time.Second, GetTimePerCu(200))
}

// --- BoundCallerRelayTimeout: a caller's lava-relay-timeout (MAG-3600) ---

func TestBoundCallerRelayTimeout(t *testing.T) {
	light := TimeoutInfo{CU: 20}                                                // budget: DefaultTimeout
	heavy := TimeoutInfo{CU: 80}                                                // budget: DefaultTimeout * 2
	hanging := TimeoutInfo{CU: 10, Hanging: true}                               // budget: DefaultTimeout * 6
	stateful := TimeoutInfo{CU: 10, Stateful: CONSISTENCY_SELECT_ALL_PROVIDERS} // budget: DefaultTimeout * 6

	// A hanging write on a chain with a 10-minute block: its own window is twice the block time plus
	// the 7s floor, so with no header it gets ~20 minutes, far above its category's 180s.
	const slowBlockHangingWindow = 20*time.Minute + 7*time.Second

	tests := []struct {
		name       string
		maxCaller  time.Duration
		requested  time.Duration
		ownWindow  time.Duration // the router's own window for the call; zero means inside its category budget
		info       TimeoutInfo
		wantWindow time.Duration
		wantOK     bool
	}{
		// Not positive: ignored, so the router's own window applies. These reached time.NewTicker
		// and ended the process before the fix.
		{name: "negative is ignored", requested: -45 * time.Second, info: light},
		{name: "minus one nanosecond is ignored", requested: -time.Nanosecond, info: light},
		{name: "zero is ignored", requested: 0, info: light},

		// Below one round trip: raised to the floor.
		{name: "one nanosecond is raised to the floor", requested: time.Nanosecond, info: light, wantWindow: MinCallerRelayTimeout, wantOK: true},
		{name: "just under the floor is raised to it", requested: MinCallerRelayTimeout - time.Nanosecond, info: light, wantWindow: MinCallerRelayTimeout, wantOK: true},

		// Inside the request's own budget: honoured as sent.
		{name: "300ms is honoured", requested: 300 * time.Millisecond, info: light, wantWindow: 300 * time.Millisecond, wantOK: true},
		{name: "5s is honoured", requested: 5 * time.Second, info: light, wantWindow: 5 * time.Second, wantOK: true},
		{name: "exactly the budget is honoured", requested: 30 * time.Second, info: light, wantWindow: 30 * time.Second, wantOK: true},
		{name: "60s fits a hanging call's budget", requested: 60 * time.Second, info: hanging, wantWindow: 60 * time.Second, wantOK: true},

		// Above it: held to the budget the router would have given the request anyway.
		{name: "45s is held to a light call's budget", requested: 45 * time.Second, info: light, wantWindow: 30 * time.Second, wantOK: true},
		{name: "45m is held to a light call's budget", requested: 45 * time.Minute, info: light, wantWindow: 30 * time.Second, wantOK: true},
		{name: "45m is held to a heavy call's budget", requested: 45 * time.Minute, info: heavy, wantWindow: 60 * time.Second, wantOK: true},
		{name: "45m is held to a hanging call's budget", requested: 45 * time.Minute, info: hanging, wantWindow: 180 * time.Second, wantOK: true},
		{name: "45m is held to a stateful call's budget", requested: 45 * time.Minute, info: stateful, wantWindow: 180 * time.Second, wantOK: true},
		{name: "the largest duration is held to the budget", requested: time.Duration(math.MaxInt64), info: light, wantWindow: 30 * time.Second, wantOK: true},

		// A call whose own window exceeds its category budget: the bound is the call's own budget.
		{name: "45m is held to a slow-block hanging call's own budget, not its category's", requested: 45 * time.Minute, ownWindow: slowBlockHangingWindow, info: stateful, wantWindow: slowBlockHangingWindow, wantOK: true},
		{name: "25m is held to that call's own budget", requested: 25 * time.Minute, ownWindow: slowBlockHangingWindow, info: stateful, wantWindow: slowBlockHangingWindow, wantOK: true},
		{name: "20m fits inside that call's own budget", requested: 20 * time.Minute, ownWindow: slowBlockHangingWindow, info: stateful, wantWindow: 20 * time.Minute, wantOK: true},
		{name: "an operator ceiling below that budget does not cut it", maxCaller: 10 * time.Minute, requested: 45 * time.Minute, ownWindow: slowBlockHangingWindow, info: stateful, wantWindow: slowBlockHangingWindow, wantOK: true},

		// An operator ceiling above the budget lets the caller stretch it, up to the ceiling.
		{name: "operator ceiling: 45s is honoured", maxCaller: 10 * time.Minute, requested: 45 * time.Second, info: light, wantWindow: 45 * time.Second, wantOK: true},
		{name: "operator ceiling: 45m is held to it", maxCaller: 10 * time.Minute, requested: 45 * time.Minute, info: light, wantWindow: 10 * time.Minute, wantOK: true},
		{name: "operator ceiling: it binds a hanging call too", maxCaller: 10 * time.Minute, requested: 45 * time.Minute, info: hanging, wantWindow: 10 * time.Minute, wantOK: true},
		// ...and one below the budget changes nothing: it never shortens.
		{name: "operator ceiling below the budget has no effect", maxCaller: 10 * time.Second, requested: 45 * time.Second, info: light, wantWindow: 30 * time.Second, wantOK: true},
		{name: "operator ceiling below the budget does not cut a window", maxCaller: 10 * time.Second, requested: 20 * time.Second, info: light, wantWindow: 20 * time.Second, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origDefault, origMax := DefaultTimeout, MaxCallerRelayTimeout
			t.Cleanup(func() { DefaultTimeout, MaxCallerRelayTimeout = origDefault, origMax })
			DefaultTimeout, MaxCallerRelayTimeout = 30*time.Second, tt.maxCaller
			ownWindow := tt.ownWindow
			if ownWindow == 0 {
				ownWindow = 2 * time.Second // a light call's window, well inside every category budget
			}

			window, ok := BoundCallerRelayTimeout(tt.requested, ownWindow, tt.info)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantWindow, window)
		})
	}
}

// The properties MAG-3600 is about, over the whole range a caller can send and for calls whose own
// window sits inside or far above their category budget: an accepted window is always positive (it
// feeds time.NewTicker); the budget built from it never exceeds max(the call's own budget,
// --max-caller-relay-timeout); and a caller who asks for at least the router's own window never
// gets a smaller budget than no header gives. The own budget is computed here from the own window,
// so the last property can fail if the bound is measured from the category alone.
func TestBoundCallerRelayTimeout_NeverZeroAndNeverPastTheBound(t *testing.T) {
	origDefault, origMax := DefaultTimeout, MaxCallerRelayTimeout
	t.Cleanup(func() { DefaultTimeout, MaxCallerRelayTimeout = origDefault, origMax })
	DefaultTimeout = 30 * time.Second

	requested := []time.Duration{
		time.Duration(math.MinInt64), -45 * time.Minute, -time.Nanosecond, 0, time.Nanosecond,
		time.Millisecond, 299 * time.Millisecond, time.Second, 29 * time.Second, 31 * time.Second,
		179 * time.Second, 181 * time.Second, 45 * time.Minute, time.Duration(math.MaxInt64),
	}
	infos := []TimeoutInfo{{CU: 1}, {CU: 50}, {CU: 100}, {Hanging: true}, {Stateful: CONSISTENCY_SELECT_ALL_PROVIDERS}}

	ownWindows := []time.Duration{time.Second, 8*time.Minute + 20*time.Second, 20*time.Minute + 7*time.Second}

	checked := 0
	for _, maxCaller := range []time.Duration{0, 10 * time.Second, 5 * time.Minute} {
		MaxCallerRelayTimeout = maxCaller
		for _, info := range infos {
			for _, ownWindow := range ownWindows {
				ownBudget := GetTimeoutForProcessing(ownWindow, info)
				for _, value := range requested {
					checked++
					window, ok := BoundCallerRelayTimeout(value, ownWindow, info)
					if !ok {
						require.LessOrEqual(t, value, time.Duration(0), "only a non-positive value may be ignored (%s)", value)
						continue
					}
					budget := GetTimeoutForProcessing(window, info)
					require.Positive(t, window, "an accepted window must be positive (%s)", value)
					require.LessOrEqual(t, budget, max(ownBudget, maxCaller),
						"lava-relay-timeout: %s stretched the budget past the bound (max-caller-relay-timeout %s, own window %s, info %+v)", value, maxCaller, ownWindow, info)
					if value >= ownWindow {
						require.GreaterOrEqual(t, budget, ownBudget,
							"lava-relay-timeout: %s asked for more than the router's own window %s and got a smaller budget than no header (%s < %s)", value, ownWindow, budget, ownBudget)
					}
				}
			}
		}
	}
	require.Equal(t, 3*len(infos)*len(ownWindows)*len(requested), checked, "every combination must have been checked")
}

// A budget under the floor cannot happen after ValidateAndCapMinRelayTimeout, which holds
// DefaultTimeout to at least 1s. Were it to, the floor wins: a window must stay positive.
func TestBoundCallerRelayTimeout_FloorWinsOverAMisconfiguredBudget(t *testing.T) {
	origDefault, origMax := DefaultTimeout, MaxCallerRelayTimeout
	t.Cleanup(func() { DefaultTimeout, MaxCallerRelayTimeout = origDefault, origMax })
	DefaultTimeout, MaxCallerRelayTimeout = 0, 0

	window, ok := BoundCallerRelayTimeout(45*time.Second, 0, TimeoutInfo{CU: 1})
	require.True(t, ok)
	require.Equal(t, MinCallerRelayTimeout, window)
}

// --- ValidateAndCapMinRelayTimeout: the MaxCallerRelayTimeout guard ---

func TestValidateAndCapMaxCallerRelayTimeout(t *testing.T) {
	tests := []struct {
		name  string
		input time.Duration
		want  time.Duration
	}{
		{name: "zero (the default) kept: callers cannot extend", input: 0, want: 0},
		{name: "a ceiling above the budget kept", input: 10 * time.Minute, want: 10 * time.Minute},
		{name: "a ceiling below the budget kept, only warned about", input: 10 * time.Second, want: 10 * time.Second},
		{name: "negative resets to zero", input: -time.Minute, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetTimeoutGlobals(t, MinimumTimePerRelayDelay, DefaultTimeout)
			origMax := MaxCallerRelayTimeout
			t.Cleanup(func() { MaxCallerRelayTimeout = origMax })
			DefaultTimeout = 30 * time.Second
			MaxCallerRelayTimeout = tt.input

			ValidateAndCapMinRelayTimeout()

			require.Equal(t, tt.want, MaxCallerRelayTimeout)
		})
	}
}
