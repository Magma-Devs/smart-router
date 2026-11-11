package connection

import (
	"context"
	"fmt"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
)

type RelayerConnectionServer struct {
	pairingtypes.UnimplementedRelayerServer
	guid uint64
}

func (rs *RelayerConnectionServer) Relay(ctx context.Context, request *pairingtypes.RelayRequest) (*pairingtypes.RelayReply, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (rs *RelayerConnectionServer) Probe(ctx context.Context, probeReq *pairingtypes.ProbeRequest) (*pairingtypes.ProbeReply, error) {
	peerAddress := common.GetIpFromGrpcContext(ctx)
	utils.FormatInfo("received probe", utils.LogAttr("incoming-ip", peerAddress))
	return &pairingtypes.ProbeReply{
		Guid: rs.guid,
	}, nil
}

func (rs *RelayerConnectionServer) RelaySubscribe(request *pairingtypes.RelayRequest, srv pairingtypes.Relayer_RelaySubscribeServer) error {
	return fmt.Errorf("unimplemented")
}
