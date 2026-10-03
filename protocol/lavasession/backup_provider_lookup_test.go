package lavasession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newBackupTestProvider(name string) *ConsumerSessionsWithProvider {
	cswp := NewConsumerSessionWithProvider(name, []*Endpoint{{NetworkAddress: grpcListener, Enabled: true, Connections: []*EndpointConnection{}}}, 999999, firstEpochHeight, int64(0))
	cswp.StaticProvider = true
	return cswp
}

// MAG-3536: the relay server counts a request on smartrouter_backup_tier_served_total when the
// provider that answered it is a backup, so the lookup must name the backup tier and nothing else.
// A primary is static too, which is why IsStaticProvider cannot answer this.
func TestIsBackupProvider(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		map[uint64]*ConsumerSessionsWithProvider{0: newBackupTestProvider("primary")},
		map[uint64]*ConsumerSessionsWithProvider{0: newBackupTestProvider("backup")},
	))

	require.True(t, csm.IsBackupProvider("backup"))
	require.False(t, csm.IsBackupProvider("primary"), "a primary is static but not a backup")
	require.True(t, csm.IsStaticProvider("primary"), "control: the static lookup cannot tell the tiers apart")
	require.False(t, csm.IsBackupProvider("unknown"))
	require.False(t, csm.IsBackupProvider(""))

	var nilCSM *ConsumerSessionManager
	require.False(t, nilCSM.IsBackupProvider("backup"))
}
