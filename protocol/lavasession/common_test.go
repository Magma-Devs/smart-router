package lavasession

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/utils"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
)

type RelayerConnectionServer struct {
	pairingtypes.UnimplementedRelayerServer
	guid uint64
}

func (rs *RelayerConnectionServer) Relay(ctx context.Context, request *pairingtypes.RelayRequest) (*pairingtypes.RelayReply, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (rs *RelayerConnectionServer) Probe(ctx context.Context, probeReq *pairingtypes.ProbeRequest) (*pairingtypes.ProbeReply, error) {
	// peerAddress := common.GetIpFromGrpcContext(ctx)
	// utils.LavaFormatInfo("received probe", utils.LogAttr("incoming-ip", peerAddress))
	return &pairingtypes.ProbeReply{
		Guid: rs.guid,
	}, nil
}

func (rs *RelayerConnectionServer) RelaySubscribe(request *pairingtypes.RelayRequest, srv pairingtypes.Relayer_RelaySubscribeServer) error {
	return fmt.Errorf("unimplemented")
}

func startServer() (*grpc.Server, net.Listener) {
	listen := ":0"
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	tlsConfig := GetTlsConfig(NetworkAddressData{})
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	pairingtypes.RegisterRelayerServer(srv, &RelayerConnectionServer{})
	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Println("test finished:", err)
		}
	}()
	return srv, lis
}

// Note that locally testing compression will probably be out performed by non compressed.
// due to the overhead of compressing it. while global communication should benefit from reduced latency.
func BenchmarkGRPCServer(b *testing.B) {
	srv, lis := startServer()
	address := lis.Addr().String()
	defer srv.Stop()
	defer lis.Close()

	csp := &ConsumerSessionsWithProvider{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, err := csp.ConnectRawClientWithTimeout(ctx, address)
		if err != nil {
			utils.LavaFormatDebug("waiting for grpc server to launch")
			continue
		}
		cancel()
		break
	}

	runBenchmark := func(b *testing.B, opts ...grpc.DialOption) {
		var tlsConf tls.Config
		tlsConf.InsecureSkipVerify = true
		credentials := credentials.NewTLS(&tlsConf)
		opts = append(opts, grpc.WithTransportCredentials(credentials))
		conn, err := grpc.DialContext(context.Background(), address, opts...)
		if err != nil {
			b.Fatalf("failed to dial server: %v", err)
		}
		defer conn.Close()

		client := pairingtypes.NewRelayerClient(conn)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			client.Probe(context.Background(), &pairingtypes.ProbeRequest{Guid: 125, SpecId: "EVMOS", ApiInterface: "jsonrpc"})
		}
	}

	b.Run("WithoutCompression", func(b *testing.B) {
		runBenchmark(b)
	})

	b.Run("WithCompression", func(b *testing.B) {
		runBenchmark(b, grpc.WithDefaultCallOptions(
			grpc.UseCompressor(gzip.Name), // Use gzip compression for outgoing messages
		))
	})

	time.Sleep(3 * time.Second)
}
