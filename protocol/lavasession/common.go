package lavasession

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"strings"
	"time"

	"github.com/gogo/status"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

var MaxSessionsAllowedPerProvider = 1000 // Max number of sessions allowed per provider, configurable via flag

func GetMaxAllowedBlockListedSessionPerProvider() int {
	return MaxSessionsAllowedPerProvider / 3
}

const (
	// MaxConsecutiveConnectionAttempts is the number of consecutive connection
	// failures after which an endpoint is backed off (endpoint.Enabled = false).
	// Raised from 5 to 50 alongside removing the per-socket health gate: with that
	// gate gone the (often sole) direct endpoint must not be disabled too eagerly on
	// a transient blip, since disabling the only endpoint reproduces the "No pairings"
	// symptom until an epoch/successful relay re-enables it. A successful relay resets
	// the counter (see Endpoint.ResetHealth), so this is a consecutive-failure budget.
	// Shared by the provider-relay path and blockProvider — this threshold gates both.
	MaxConsecutiveConnectionAttempts = 50
	// maxReenableProbeFlaps caps how far the probe's re-enable hysteresis escalates when an endpoint
	// keeps passing cheap polls (which drive re-enable) but failing real relays (which drive the
	// disable above). Each such re-enable→re-disable flap escalates the next re-enable's K by a power
	// of two (reEnableAfterK << flaps); with the default K=3 that is 3 → 6 → 12 cycles. Capped at 2 so
	// even a perpetually-flapping endpoint is re-probed within a bounded window (~60s at a 5s cadence)
	// — deliberately far below the ~15-minute epoch re-probe, so the dampening never parks a node that
	// is genuinely healthy for cheap traffic. A successful relay (Endpoint.ResetHealth) decays it to 0.
	maxReenableProbeFlaps                            uint64 = 2
	TimeoutForEstablishingAConnection                       = 1500 * time.Millisecond // 1.5 seconds
	MaximumNumberOfFailuresAllowedPerConsumerSession        = 15
	RelayNumberIncrement                                    = 1
	unixPrefix                                              = "unix:"
)

func IsSessionSyncLoss(err error) bool {
	code := status.Code(err)
	return code == codes.Code(SessionOutOfSyncGRPCCode) || errors.Is(err, SessionOutOfSyncError)
}

func ConnectGRPCClient(ctx context.Context, address string, allowInsecure bool, skipTLS bool, allowCompression bool) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if skipTLS {
		// Skip TLS encryption completely
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// Use TLS with optional server verification
		var tlsConf tls.Config
		if allowInsecure {
			tlsConf.InsecureSkipVerify = true // Allows self-signed certificates
		}
		credentials := credentials.NewTLS(&tlsConf)
		opts = append(opts, grpc.WithTransportCredentials(credentials))
	}

	opts = append(opts, grpc.WithBlock(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(chainproxy.MaxCallRecvMsgSize)))

	if strings.HasPrefix(address, unixPrefix) {
		// Unix socket
		socketPath := strings.TrimPrefix(address, unixPrefix)
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return net.Dial("unix", socketPath)
		}))
	} else {
		// TCP socket
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		}))
	}

	// allow gzip compression for grpc.
	if allowCompression {
		opts = append(opts, grpc.WithDefaultCallOptions(
			grpc.UseCompressor(gzip.Name), // Use gzip compression for provider consumer communication
		))
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	return conn, err
}

func GenerateSelfSignedCertificate() (tls.Certificate, error) {
	// Generate a private key
	utils.LavaFormatWarning("Warning: Using Self signed certificate is not recommended, this will not allow https connections to be established", nil)
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	// Create a self-signed certificate template
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0), // Valid for 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Generate the self-signed certificate using the private key and template
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, err
	}

	// Create a tls.Certificate using the private key and certificate bytes
	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  privateKey,
	}

	return cert, nil
}

func GetCaCertificate(serverCertPath, serverKeyPath string) (*tls.Config, error) {
	serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		ClientAuth:   tls.NoClientCert,
		Certificates: []tls.Certificate{serverCert},
	}, nil
}

func GetSelfSignedConfig() (*tls.Config, error) {
	cert, err := GenerateSelfSignedCertificate()
	if err != nil {
		return nil, utils.LavaFormatError("failed to generate TLS certificate", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}, nil
}

func GetTlsConfig(networkAddress NetworkAddressData) *tls.Config {
	var tlsConfig *tls.Config
	var err error
	if networkAddress.CertPem != "" {
		utils.LavaFormatInfo("Running with TLS certificate", utils.Attribute{Key: "cert", Value: networkAddress.CertPem}, utils.Attribute{Key: "key", Value: networkAddress.KeyPem})
		tlsConfig, err = GetCaCertificate(networkAddress.CertPem, networkAddress.KeyPem)
		if err != nil {
			utils.LavaFormatFatal("failed to generate TLS certificate", err)
		}
	} else {
		tlsConfig, err = GetSelfSignedConfig()
		if err != nil {
			utils.LavaFormatFatal("failed GetSelfSignedConfig", err)
		}
	}
	return tlsConfig
}
