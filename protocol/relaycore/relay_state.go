package relaycore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
)

type RelayParserInf interface {
	ParseRelay(
		ctx context.Context,
		url string,
		req string,
		connectionType string,
		dappID string,
		consumerIp string,
		metadata []pairingtypes.Metadata,
	) (protocolMessage chainlib.ProtocolMessage, err error)
}

// ArchiveStatus is the per-request state that survives a retry. One flag: whether the
// earliest-block re-parse has already run, which must happen at most once per request.
//
// Whether the request IS archive is not held here. It lives on the request's extensions, which is
// where selection reads it from; a copy on the side had no reader and could only disagree.
type ArchiveStatus struct {
	isEarliestUsed atomic.Bool
}

func (as *ArchiveStatus) Copy() *ArchiveStatus {
	archiveStatus := &ArchiveStatus{}
	archiveStatus.isEarliestUsed.Store(as.isEarliestUsed.Load())
	return archiveStatus
}

type RelayState struct {
	archiveStatus   *ArchiveStatus
	stateNumber     int
	protocolMessage chainlib.ProtocolMessage
	lock            sync.RWMutex
}

func GetEmptyRelayState(protocolMessage chainlib.ProtocolMessage) *RelayState {
	archiveStatus := &ArchiveStatus{}
	archiveStatus.isEarliestUsed.Store(true)
	return &RelayState{
		protocolMessage: protocolMessage,
		archiveStatus:   archiveStatus,
	}
}

func NewRelayState(protocolMessage chainlib.ProtocolMessage, stateNumber int, archiveStatus *ArchiveStatus) *RelayState {
	if archiveStatus == nil {
		utils.LavaFormatError("misuse detected archiveStatus is nil", nil, utils.Attribute{Key: "protocolMessage.GetApi", Value: protocolMessage.GetApi()})
		archiveStatus = &ArchiveStatus{}
	}
	rs := &RelayState{
		protocolMessage: protocolMessage,
		stateNumber:     stateNumber,
		archiveStatus:   archiveStatus,
	}
	return rs
}

func (rs *RelayState) GetIsEarliestUsed() bool {
	if rs == nil || rs.archiveStatus == nil {
		return true
	}
	return rs.archiveStatus.isEarliestUsed.Load()
}

func (rs *RelayState) SetIsEarliestUsed() {
	if rs == nil || rs.archiveStatus == nil {
		return
	}
	rs.archiveStatus.isEarliestUsed.Store(true)
}

func (rs *RelayState) GetStateNumber() int {
	if rs == nil {
		return 0
	}
	return rs.stateNumber
}

func (rs *RelayState) GetProtocolMessage() chainlib.ProtocolMessage {
	if rs == nil {
		return nil
	}
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.protocolMessage
}

func (rs *RelayState) GetArchiveStatus() *ArchiveStatus {
	if rs == nil || rs.archiveStatus == nil {
		return nil
	}
	return rs.archiveStatus.Copy()
}

func (rs *RelayState) SetProtocolMessage(protocolMessage chainlib.ProtocolMessage) {
	if rs == nil {
		return
	}
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.protocolMessage = protocolMessage
}
