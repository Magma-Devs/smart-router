package relaycore

import (
	"context"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// backupTierSender answers HasUntriedBackup from a field and records what it was asked with.
type backupTierSender struct {
	stubRelaySender
	untried   bool
	asked     int
	askedWith *lavasession.UsedProviders
}

func (s *backupTierSender) HasUntriedBackup(_ context.Context, _ chainlib.ProtocolMessage, usedProviders *lavasession.UsedProviders) bool {
	s.asked++
	s.askedWith = usedProviders
	return s.untried
}

func newBackupTierStateMachine(t *testing.T, sender *backupTierSender) (*UnifiedRelayStateMachine, *lavasession.UsedProviders) {
	t.Helper()
	pm := newRetryTestProtocolMessage(t, nil, nil)
	usedProviders := lavasession.NewUsedProviders(nil)
	sm, err := NewUnifiedRelayStateMachine(
		context.Background(),
		usedProviders,
		sender,
		pm,
		nil,
		false,
		StateMachineConfig{MaxRetries: 10, SendRelayAttempts: 3},
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	usm, ok := sm.(*UnifiedRelayStateMachine)
	require.True(t, ok)
	return usm, usedProviders
}

// TestDecisionInput_CarriesTheSendersBackupTierAnswer_MAG3978: the policy can only wait for a
// backup it is told about. The gotResults path asks the sender, with this request's used
// providers, so the answer is about the providers this request has left; a hedge does not ask,
// because it never checks the tolerance the answer feeds.
func TestDecisionInput_CarriesTheSendersBackupTierAnswer_MAG3978(t *testing.T) {
	for _, untried := range []bool{true, false} {
		t.Run(fmt.Sprintf("the sender says %v", untried), func(t *testing.T) {
			sender := &backupTierSender{stubRelaySender: stubRelaySender{t: t}, untried: untried}
			sm, usedProviders := newBackupTierStateMachine(t, sender)

			input := sm.buildDecisionInput(false)
			require.Equal(t, untried, input.UntriedBackupRemains)
			require.Equal(t, 1, sender.asked, "the gotResults path asks once per decision")
			require.Same(t, usedProviders, sender.askedWith, "the sender must see this request's used providers, or it answers for an empty request")
		})
	}

	t.Run("a hedge does not ask", func(t *testing.T) {
		sender := &backupTierSender{stubRelaySender: stubRelaySender{t: t}, untried: true}
		sm, _ := newBackupTierStateMachine(t, sender)

		input := sm.buildDecisionInput(true)
		require.False(t, input.UntriedBackupRemains)
		require.Zero(t, sender.asked)
	})
}
