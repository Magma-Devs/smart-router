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
}

// Both flags are registered on the shipped command and reach viper. Parse would
// reject them outright if they were not.
func TestRealCommand_WebsocketTunablesFromFlags(t *testing.T) {
	wireLikeRunE(t, "",
		"--"+common.WebsocketKeepAliveIntervalFlag, "5s",
		"--"+common.LimitWebsocketIdleTimeFlag, "90",
	)

	require.Equal(t, 5*time.Second, viper.GetDuration(common.WebsocketKeepAliveIntervalFlag))
	require.Equal(t, int64(90), viper.GetInt64(common.LimitWebsocketIdleTimeFlag))
}

// Operators configure the router from a YAML file far more often than from the
// command line, and these keys are read the same way either way.
func TestRealCommand_WebsocketTunablesFromYAML(t *testing.T) {
	wireLikeRunE(t, common.WebsocketKeepAliveIntervalFlag+": 45s\n"+
		common.LimitWebsocketIdleTimeFlag+": 120\n")

	require.Equal(t, 45*time.Second, viper.GetDuration(common.WebsocketKeepAliveIntervalFlag))
	require.Equal(t, int64(120), viper.GetInt64(common.LimitWebsocketIdleTimeFlag))
}
