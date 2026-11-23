package routersession

import (
	"errors"

	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// ErrNoMethodDefinition is the router holding no definition of a gRPC method: the provider's own
// lookup failed, or is waiting out an earlier failure (GRPCDescriptorResolver), and no other
// provider of the chain has given one either. It is not the provider's failure. Verification skips
// a check that needs the definition, and the tracker reports the provider's height unknown, not the
// provider down (MAG-5134).
var ErrNoMethodDefinition = errors.New("no definition of the gRPC method from any provider of the chain")

// ChainDescriptors holds the gRPC method definitions the router has learned from any provider of
// one chain (MAG-5134). A definition describes the chain's API, not the node that served it, so a
// provider whose own lookup fails, or that offers no lookup at all, is read with one another
// provider gave.
//
// A provider's own definition still comes first: providers on different node builds can differ,
// and a JSON request converted with an older build's definition could lose a field the newer node
// knows. This is the fallback, never the first choice.
//
// One per chain, owned by the chain's gRPC parser and shared by every resolver of the chain and by
// verification. A nil *ChainDescriptors shares nothing.
type ChainDescriptors struct {
	methods common.SafeSyncMap[string, *desc.MethodDescriptor]
}

// NewChainDescriptors returns an empty store for one chain.
func NewChainDescriptors() *ChainDescriptors {
	return &ChainDescriptors{}
}

// Method returns the definition of service.method that some provider of the chain gave.
func (c *ChainDescriptors) Method(service, method string) (*desc.MethodDescriptor, bool) {
	if c == nil {
		return nil, false
	}
	methodDescriptor, found, _ := c.methods.Load(service + "." + method)
	return methodDescriptor, found && methodDescriptor != nil
}

// AddService records every method of a service resolved from a provider of the chain. A later
// provider's definition replaces an earlier one's.
func (c *ChainDescriptors) AddService(serviceDescriptor *desc.ServiceDescriptor) {
	if c == nil || serviceDescriptor == nil {
		return
	}
	service := serviceDescriptor.GetFullyQualifiedName()
	for _, method := range serviceDescriptor.GetMethods() {
		c.methods.Store(service+"."+method.GetName(), method)
	}
}
