package rpcsmartrouter_test

// Acceptance for the two websocket tunables RunE reads through viper rather than
// binding to a variable with Int64Var/DurationVar.
//
// A pointer binding cannot silently fail. These two are read with
// viper.GetDuration/GetInt64, so a missing BindPFlags, a renamed key or a flag
// registered on the wrong command hands the code a zero instead of an error —
// and zero is meaningful for both: it disables the keep-alive ping and the idle
// reaper outright. Nothing else in the process would report it.
//
// Same sequence and same caveats as resp_cobra_viper_test.go: the real cobra
// command, the real global viper, no router started and no network touched.

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// Absent configuration must produce the shipped defaults, not zero.
func TestRealCommand_WebsocketTunablesDefaultWhenUnset(t *testing.T) {
	wireLikeRunE(t, "")

	require.Equal(t, chainlib.DefaultWebSocketKeepAliveInterval, viper.GetDuration(common.WebsocketKeepAliveIntervalFlag),
		"--%s must fall back to its registered default; 0 would disable keep-alive pings", common.WebsocketKeepAliveIntervalFlag)
	require.Equal(t, chainlib.DefaultMaxIdleTimeInSeconds, viper.GetInt64(common.LimitWebsocketIdleTimeFlag),
		"--%s must fall back to its registered default; 0 would disable the idle reaper", common.LimitWebsocketIdleTimeFlag)
	require.Equal(t, chainlib.DefaultWebSocketWriteTimeout, viper.GetDuration(common.WebsocketWriteTimeoutFlag),
		"--%s must fall back to its registered default; 0 would remove the write deadline", common.WebsocketWriteTimeoutFlag)
}

// Both flags are registered on the shipped command and reach viper. Parse would
// reject them outright if they were not.
func TestRealCommand_WebsocketTunablesFromFlags(t *testing.T) {
	wireLikeRunE(t, "",
		"--"+common.WebsocketKeepAliveIntervalFlag, "5s",
		"--"+common.LimitWebsocketIdleTimeFlag, "90",
		"--"+common.WebsocketWriteTimeoutFlag, "3s",
	)

	require.Equal(t, 5*time.Second, viper.GetDuration(common.WebsocketKeepAliveIntervalFlag))
	require.Equal(t, int64(90), viper.GetInt64(common.LimitWebsocketIdleTimeFlag))
	require.Equal(t, 3*time.Second, viper.GetDuration(common.WebsocketWriteTimeoutFlag))
}

// Operators configure the router from a YAML file far more often than from the
// command line, and these keys are read the same way either way.
func TestRealCommand_WebsocketTunablesFromYAML(t *testing.T) {
	wireLikeRunE(t, common.WebsocketKeepAliveIntervalFlag+": 45s\n"+
		common.LimitWebsocketIdleTimeFlag+": 120\n"+
		common.WebsocketWriteTimeoutFlag+": 7s\n")

	require.Equal(t, 45*time.Second, viper.GetDuration(common.WebsocketKeepAliveIntervalFlag))
	require.Equal(t, int64(120), viper.GetInt64(common.LimitWebsocketIdleTimeFlag))
	require.Equal(t, 7*time.Second, viper.GetDuration(common.WebsocketWriteTimeoutFlag))
}

// A bare number in YAML is read as nanoseconds, so `websocket-keep-alive-interval: 30`
// means 30ns — while `limit-websocket-connection-idle-time: 120` beside it genuinely
// means 120 seconds. An operator consistent across the block gets a ping ticker nine
// orders of magnitude too fast and a write deadline that fails the first frame on every
// connection, silently. Startup rejects the unitless form instead.
func TestRealCommand_WebsocketDurationsRejectBareNumbers(t *testing.T) {
	for _, flagName := range []string{common.WebsocketKeepAliveIntervalFlag, common.WebsocketWriteTimeoutFlag} {
		t.Run(flagName, func(t *testing.T) {
			wireLikeRunE(t, flagName+": 30\n")

			// The value really is nanoseconds, which is what makes the check necessary.
			require.Equal(t, 30*time.Nanosecond, viper.GetDuration(flagName),
				"a bare number is read as nanoseconds; that is the trap being guarded")

			err := common.ValidateDurationConfigValues(viper.GetViper(),
				common.WebsocketKeepAliveIntervalFlag, common.WebsocketWriteTimeoutFlag)
			require.Error(t, err, "startup must refuse a unitless duration")
			require.Contains(t, err.Error(), flagName)
		})
	}
}

// The forms an operator is meant to use must all pass: a unit in YAML, an explicit
// flag, and the registered default with nothing set. The seconds-valued neighbour is
// not a duration key and must not be caught by the same check.
func TestRealCommand_WebsocketDurationsAcceptEveryValidForm(t *testing.T) {
	durations := []string{common.WebsocketKeepAliveIntervalFlag, common.WebsocketWriteTimeoutFlag}

	for _, tc := range []struct {
		name  string
		yaml  string
		flags []string
	}{
		{"defaults, nothing set", "", nil},
		{"yaml with units", common.WebsocketKeepAliveIntervalFlag + ": 45s\n" + common.WebsocketWriteTimeoutFlag + ": 7s\n", nil},
		{"explicit flags", "", []string{"--" + common.WebsocketKeepAliveIntervalFlag, "5s", "--" + common.WebsocketWriteTimeoutFlag, "3s"}},
		{
			"alongside the seconds-valued neighbour as a bare number",
			common.LimitWebsocketIdleTimeFlag + ": 120\n" + common.WebsocketKeepAliveIntervalFlag + ": 45s\n",
			nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wireLikeRunE(t, tc.yaml, tc.flags...)
			require.NoError(t, common.ValidateDurationConfigValues(viper.GetViper(), durations...))
		})
	}
}
