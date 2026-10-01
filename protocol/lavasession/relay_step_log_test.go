package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// "VALIDATING PROVIDERS" is written once per GetSessions call. A write keeps it at Info so a
// transaction is followed at the default level; a read writes it at Debug. The helper is covered
// on its own — this pins that GetSessions hands it the relay's own stateful value.
func TestGetSessions_ValidatingProvidersLevelFollowsStateful(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stateful     uint32
		wantLevel    string
		wantStateful string
	}{
		{name: "write", stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS, wantLevel: "info", wantStateful: "1"},
		{name: "read", stateful: common.NO_STATE, wantLevel: "debug", wantStateful: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			csm := CreateConsumerSessionManager()
			require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

			records := captureLogs(t, func() {
				_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil),
					servicedBlockNumber, "", nil, tc.stateful, 0, "", "")
				require.NoError(t, err)
			})

			line := findLogRecord(records, "VALIDATING PROVIDERS")
			require.NotNil(t, line, "the line must still be written")
			require.Equal(t, tc.wantLevel, line["level"])
			require.Equal(t, tc.wantStateful, line["stateful"])
		})
	}
}
