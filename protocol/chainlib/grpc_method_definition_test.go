package chainlib

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

func chainProxyWithStore(chain *routersession.ChainDescriptors, reflectionTimeout time.Duration) *GrpcChainProxy {
	return &GrpcChainProxy{
		BaseChainProxy:   BaseChainProxy{ChainID: "TEST", NodeUrl: common.NodeUrl{GrpcConfig: common.GrpcConfig{ReflectionTimeout: reflectionTimeout}}},
		descriptorsCache: &common.SafeSyncMap[string, *desc.MethodDescriptor]{},
		chainDescriptors: chain,
	}
}

// emptySource is a node that gives no definition of anything.
func emptySource(t *testing.T) grpcurl.DescriptorSource {
	t.Helper()
	source, err := grpcurl.DescriptorSourceFromFileDescriptors()
	require.NoError(t, err)
	return source
}

// blockingSource is a node that accepts the reflection stream and never answers.
type blockingSource struct {
	grpcurl.DescriptorSource
	release chan struct{}
}

func (b blockingSource) FindSymbol(string) (desc.Descriptor, error) {
	<-b.release
	return nil, errors.New("released")
}

// Verification uses a node's own definition, and adds it to the chain's store for the chain's other
// providers (MAG-5134).
func TestGrpcChainProxy_ResolveMethod_OwnDefinitionIsSharedWithTheChain(t *testing.T) {
	chain := routersession.NewChainDescriptors()
	own, err := grpcurl.DescriptorSourceFromFileDescriptors(queryServiceFile(t, "bank"))
	require.NoError(t, err)
	cp := chainProxyWithStore(chain, 0)

	methodDescriptor, source, err := cp.resolveMethod(context.Background(), own, "bank.Query/Params")
	require.NoError(t, err)
	require.Same(t, own, source, "the node's own source reads the reply")
	shared, ok := chain.Method("bank.Query", "Params")
	require.True(t, ok, "the definition is now the chain's")
	require.Same(t, methodDescriptor, shared)
}

// A node that gives no definition is verified with another provider's, read through that
// definition's own files, so the reply can be formatted.
func TestGrpcChainProxy_ResolveMethod_FallsBackToAnotherProvidersDefinition(t *testing.T) {
	chain := routersession.NewChainDescriptors()
	other, err := grpcurl.DescriptorSourceFromFileDescriptors(queryServiceFile(t, "bank"))
	require.NoError(t, err)
	_, _, err = chainProxyWithStore(chain, 0).resolveMethod(context.Background(), other, "bank.Query/Params")
	require.NoError(t, err)

	methodDescriptor, source, err := chainProxyWithStore(chain, 0).resolveMethod(context.Background(), emptySource(t), "bank.Query/Params")
	require.NoError(t, err)
	require.Equal(t, "bank.ParamsRequest", methodDescriptor.GetInputType().GetFullyQualifiedName())
	found, err := source.FindSymbol("bank.ParamsResponse")
	require.NoError(t, err, "the source reads the shared definition's messages")
	require.Equal(t, "bank.ParamsResponse", found.GetFullyQualifiedName())
}

// With no definition from anyone, verification gets ErrNoMethodDefinition, which it reads as a check
// it could not run rather than one the node failed.
func TestGrpcChainProxy_ResolveMethod_NoneAnywhereIsNoMethodDefinition(t *testing.T) {
	_, _, err := chainProxyWithStore(routersession.NewChainDescriptors(), 0).resolveMethod(context.Background(), emptySource(t), "bank.Query/Params")
	require.ErrorIs(t, err, routersession.ErrNoMethodDefinition)
	require.True(t, noMethodDefinition(err))
	require.True(t, stopValidateRetries(err), "asking again in the same pass cannot help")
}

// A node that accepts the reflection stream and never answers held the whole verification attempt
// (30 s). The lookup is now waited on for reflection-timeout only (MAG-5134).
func TestGrpcChainProxy_ResolveMethod_LookupIsBoundedByReflectionTimeout(t *testing.T) {
	const reflectionTimeout = 200 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	cp := chainProxyWithStore(routersession.NewChainDescriptors(), reflectionTimeout)

	start := time.Now()
	_, _, err := cp.resolveMethod(context.Background(), blockingSource{DescriptorSource: emptySource(t), release: release}, "bank.Query/Params")
	took := time.Since(start)
	require.ErrorIs(t, err, routersession.ErrNoMethodDefinition)
	require.Contains(t, err.Error(), "reflection-timeout")
	require.Less(t, took, 10*reflectionTimeout, "returned at the reflection-timeout, not at the attempt's deadline")
}

// A check that cannot run because no provider of the chain has given a definition of the method it
// calls is skipped with a warning, and the provider is admitted, as with a configured skip: refusing
// it left a node that offers no lookup unable to serve at all (MAG-5134). A plain failure still
// refuses it.
func TestValidate_CheckWithoutMethodDefinitionIsSkipped(t *testing.T) {
	cf := newEthChainFetcherWithRouterErr(t, fmt.Errorf("verify: %w", routersession.ErrNoMethodDefinition))
	cf.endpoint.NodeUrls = []common.NodeUrl{{Url: "http://127.0.0.1:1"}}
	results, admissionErr := cf.ValidateReport(context.Background())
	require.NoError(t, admissionErr, "every check was skipped, none failed")
	require.Len(t, results, 1)
	require.Empty(t, results[0].Verifications, "a skipped check has no verdict")

	failing := newEthChainFetcherWithRouterErr(t, errors.New("node down"))
	failing.endpoint.NodeUrls = []common.NodeUrl{{Url: "http://127.0.0.1:1"}}
	_, admissionErr = failing.ValidateReport(context.Background())
	require.Error(t, admissionErr, "a check the node failed still refuses it")
}

// A client request whose block the parser cannot read for want of a method definition is reported
// once, with its reason, however many times its block is parsed (MAG-4563).
func TestGrpcMessage_UnreadBlockIsReportedOncePerRequest(t *testing.T) {
	parser, err := NewGrpcChainParser()
	require.NoError(t, err)
	var reasons []string
	parser.SetUnreadBlockObserver(func(reason string) { reasons = append(reasons, reason) })

	registry := dyncodec.NewRegistry(dyncodec.NewSnapshotRemote(nil))
	message := rpcInterfaceMessages.GrpcMessage{
		Msg:           []byte{0x08, 0x01},
		Path:          "bank.Query/Params",
		Registry:      registry,
		Codec:         dyncodec.NewCodec(registry),
		OnUnreadBlock: parser.unreadBlockOnce(),
	}
	require.Nil(t, message.GetParams())
	require.Nil(t, message.GetParams())
	require.Equal(t, []string{dyncodec.UnreadBlockNoSnapshot}, reasons, "no snapshot held, reported once")
}
