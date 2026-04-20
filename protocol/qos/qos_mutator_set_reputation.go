package qos

import (
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// Mutator to set usage for a session
type QoSMutatorSetReputation struct {
	*QoSMutatorBase
	report *pairingtypes.QualityOfServiceReport
}

func (qoSMutatorSetReputation *QoSMutatorSetReputation) Mutate(report *QoSReport) {
	report.lastReputationQoSReport = qoSMutatorSetReputation.report
}
