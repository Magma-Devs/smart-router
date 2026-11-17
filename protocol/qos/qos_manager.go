package qos

import (
	"sync"
	"time"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

type QoSManager struct {
	qosReports map[uint64]map[int64]*QoSReport // first key is the epoch, second key is the session id
	lock       sync.RWMutex
}

func NewQoSManager() *QoSManager {
	qosManager := &QoSManager{}
	qosManager.qosReports = make(map[uint64]map[int64]*QoSReport)
	return qosManager
}

// Getting previous epoch and cleaning anything below that.
func (qosManager *QoSManager) CleanPurgedEpochs(previousEpoch uint64) {
	qosManager.lock.Lock()
	defer qosManager.lock.Unlock()
	for epoch := range qosManager.qosReports {
		if epoch < previousEpoch {
			delete(qosManager.qosReports, epoch)
		}
	}
}

// ForgetSession drops every report filed for sessionId, under whichever epoch it was filed: a
// session's failures go under the epoch it was created in, its successes under the current one.
// For a session that is gone for good, such as a retired one its provider reclaimed (MAG-5063).
func (qosManager *QoSManager) ForgetSession(sessionId int64) {
	qosManager.lock.Lock()
	defer qosManager.lock.Unlock()
	for _, sessions := range qosManager.qosReports {
		delete(sessions, sessionId)
	}
}

func (qosManager *QoSManager) fetchOrSetSessionFromMap(epoch uint64, sessionId int64) *QoSReport {
	qosManager.lock.Lock()
	defer qosManager.lock.Unlock()
	if qosManager.qosReports[epoch] == nil {
		qosManager.qosReports[epoch] = make(map[int64]*QoSReport)
	}
	if qosManager.qosReports[epoch][sessionId] == nil {
		qosManager.qosReports[epoch][sessionId] = &QoSReport{}
	}
	return qosManager.qosReports[epoch][sessionId]
}

func (qosManager *QoSManager) createQoSMutatorBase(epoch uint64, sessionId int64) *QoSMutatorBase {
	qosMutatorBase := &QoSMutatorBase{
		epoch:     epoch,
		sessionId: sessionId,
	}
	return qosMutatorBase
}

func (qm *QoSManager) mutate(mutator Mutator) {
	qosReport := qm.fetchOrSetSessionFromMap(mutator.GetEpochAndSessionId())
	qosReport.mutate(mutator)
}

func (qm *QoSManager) DegradeAvailability(epoch uint64, sessionId int64) {
	qm.mutate(&QoSMutatorDegradeAvailability{
		QoSMutatorBase: qm.createQoSMutatorBase(epoch, sessionId),
	})
}

func (qosManager *QoSManager) CalculateQoS(epoch uint64, sessionId int64, providerAddress string, latency, expectedLatency time.Duration, blockHeightDiff int64, numOfProviders int, servicersToCount int64) {
	qosManager.mutate(&QoSMutatorRelaySuccess{
		QoSMutatorBase:   qosManager.createQoSMutatorBase(epoch, sessionId),
		providerAddress:  providerAddress,
		latency:          latency,
		expectedLatency:  expectedLatency,
		blockHeightDiff:  blockHeightDiff,
		numOfProviders:   numOfProviders,
		servicersToCount: servicersToCount,
	})
}

func (qosManager *QoSManager) AddFailedRelay(epoch uint64, sessionId int64) {
	qosManager.mutate(&QoSMutatorRelayFailure{
		QoSMutatorBase: qosManager.createQoSMutatorBase(epoch, sessionId),
	})
}

func (qosManager *QoSManager) SetLastReputationQoSReport(epoch uint64, sessionId int64, report *pairingtypes.QualityOfServiceReport) {
	qosManager.mutate(&QoSMutatorSetReputation{
		QoSMutatorBase: qosManager.createQoSMutatorBase(epoch, sessionId),
		report:         report,
	})
}

func (qosManager *QoSManager) getQoSReport(epoch uint64, sessionId int64) *QoSReport {
	qosManager.lock.RLock()
	defer qosManager.lock.RUnlock()
	if qosManager.qosReports[epoch] == nil {
		return nil
	}
	return qosManager.qosReports[epoch][sessionId]
}

func (qosManager *QoSManager) GetLastQoSReport(epoch uint64, sessionId int64) *pairingtypes.QualityOfServiceReport {
	qosReport := qosManager.getQoSReport(epoch, sessionId)
	if qosReport == nil {
		return nil
	}
	return qosReport.getLastQoSReport()
}

func (qosManager *QoSManager) GetLastReputationQoSReport(epoch uint64, sessionId int64) *pairingtypes.QualityOfServiceReport {
	qosReport := qosManager.getQoSReport(epoch, sessionId)
	if qosReport == nil {
		return nil
	}
	return qosReport.getLastReputationQoSReport()
}

func (qosManager *QoSManager) GetAnsweredRelays(epoch uint64, sessionId int64) uint64 {
	qosReport := qosManager.getQoSReport(epoch, sessionId)
	if qosReport == nil {
		return 0
	}
	return qosReport.getAnsweredRelays()
}

func (qosManager *QoSManager) GetTotalRelays(epoch uint64, sessionId int64) uint64 {
	qosReport := qosManager.getQoSReport(epoch, sessionId)
	if qosReport == nil {
		return 0
	}
	return qosReport.getTotalRelays()
}

func (qosManager *QoSManager) GetSyncScoreSum(epoch uint64, sessionId int64) int64 {
	qosReport := qosManager.getQoSReport(epoch, sessionId)
	if qosReport == nil {
		return 0
	}
	return qosReport.getSyncScoreSum()
}

func (qosManager *QoSManager) GetTotalSyncScore(epoch uint64, sessionId int64) int64 {
	qosReport := qosManager.getQoSReport(epoch, sessionId)
	if qosReport == nil {
		return 0
	}
	return qosReport.getTotalSyncScore()
}
