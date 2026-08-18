package rpcInterfaceMessages

import (
	"fmt"
	"sync"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

// protosetCache memoizes parsed descriptor sets by path.
//
// A protoset is a static artifact of the config — the Concordium one is ~335KB —
// and descriptor lookups happen per connection, per uncached method. Without this
// the same file is read and unmarshalled on every miss for the life of the process.
//
// Failures are cached alongside successes deliberately: a path that is missing or
// corrupt at boot stays that way, and re-reading it on every relay turns one config
// mistake into per-request filesystem traffic. Editing a protoset requires a router
// restart, which is already true of every other field in the config.
var protosetCache sync.Map // map[string]*protosetEntry

type protosetEntry struct {
	once   sync.Once
	source grpcurl.DescriptorSource
	err    error
}

// LoadProtoset returns the descriptor source backed by the protoset at path,
// parsing it at most once per path per process.
func LoadProtoset(path string) (grpcurl.DescriptorSource, error) {
	cached, _ := protosetCache.LoadOrStore(path, &protosetEntry{})
	entry, ok := cached.(*protosetEntry)
	if !ok {
		return nil, utils.LavaFormatError("corrupt protoset cache entry", nil,
			utils.LogAttr("descriptor-set-path", path))
	}

	entry.once.Do(func() {
		source, err := grpcurl.DescriptorSourceFromProtoSets(path)
		if err != nil {
			entry.err = utils.LavaFormatError("failed loading gRPC descriptor set", err,
				utils.LogAttr("descriptor-set-path", path))
			return
		}
		entry.source = source
		utils.LavaFormatDebug("loaded gRPC descriptor set",
			utils.LogAttr("descriptor-set-path", path))
	})

	return entry.source, entry.err
}

// ResetProtosetCacheForTest drops every memoized protoset. Tests that write a
// descriptor set to a temp path and expect it to be re-read must call this;
// production never does.
func ResetProtosetCacheForTest() {
	protosetCache.Range(func(key, _ any) bool {
		protosetCache.Delete(key)
		return true
	})
}

// compositeDescriptorSource resolves symbols against a protoset first and falls
// back to server reflection. This is "hybrid" mode.
//
// File-first, not reflection-first, is deliberate. The chains that need hybrid at
// all are the ones whose upstream reflection is absent, partial, or rate-limited —
// asking the node first would pay that cost on every lookup only to arrive at an
// answer that was on local disk the whole time. File-first makes the common path
// free and leaves reflection as what it actually is here: a supplement for symbols
// the protoset predates.
type compositeDescriptorSource struct {
	file   grpcurl.DescriptorSource
	server grpcurl.DescriptorSource
}

var _ grpcurl.DescriptorSource = compositeDescriptorSource{}

func (c compositeDescriptorSource) ListServices() ([]string, error) {
	services, err := c.file.ListServices()
	if err != nil {
		return c.server.ListServices()
	}

	// A reflection failure is not fatal in hybrid mode — the protoset already
	// answered. Report what we have rather than losing it to the node being down.
	serverServices, serverErr := c.server.ListServices()
	if serverErr != nil {
		utils.LavaFormatDebug("hybrid descriptor source: reflection ListServices failed, using protoset only",
			utils.LogAttr("error", serverErr))
		return services, nil
	}

	seen := make(map[string]struct{}, len(services)+len(serverServices))
	merged := make([]string, 0, len(services)+len(serverServices))
	for _, service := range append(services, serverServices...) {
		if _, duplicate := seen[service]; duplicate {
			continue
		}
		seen[service] = struct{}{}
		merged = append(merged, service)
	}
	return merged, nil
}

func (c compositeDescriptorSource) FindSymbol(fullyQualifiedName string) (desc.Descriptor, error) {
	descriptor, fileErr := c.file.FindSymbol(fullyQualifiedName)
	if fileErr == nil {
		return descriptor, nil
	}

	descriptor, serverErr := c.server.FindSymbol(fullyQualifiedName)
	if serverErr == nil {
		return descriptor, nil
	}

	// Both halves missed. Surface both causes: in practice one of them is the
	// actionable one (stale protoset vs. node not serving reflection) and which
	// it is depends on the deployment.
	return nil, utils.LavaFormatError("symbol not found in protoset or via reflection", nil,
		utils.LogAttr("symbol", fullyQualifiedName),
		utils.LogAttr("protoset_error", fileErr),
		utils.LogAttr("reflection_error", serverErr))
}

func (c compositeDescriptorSource) AllExtensionsForType(typeName string) ([]*desc.FieldDescriptor, error) {
	fileExtensions, fileErr := c.file.AllExtensionsForType(typeName)
	serverExtensions, serverErr := c.server.AllExtensionsForType(typeName)

	if fileErr != nil && serverErr != nil {
		return nil, fileErr
	}

	seen := make(map[string]struct{}, len(fileExtensions)+len(serverExtensions))
	merged := make([]*desc.FieldDescriptor, 0, len(fileExtensions)+len(serverExtensions))
	for _, extension := range append(fileExtensions, serverExtensions...) {
		name := extension.GetFullyQualifiedName()
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		merged = append(merged, extension)
	}
	return merged, nil
}

// DescriptorSourceForNode picks the descriptor source implied by a node's
// grpc-config. serverSource is the reflection-backed source the caller already
// holds; in "file" mode it is never consulted, so a node that does not serve
// reflection is never asked.
//
// This is chain-agnostic on purpose: nothing here knows which chain it is serving.
// Concordium is the case that forced it (its gateways serve reflection for Health
// only), but public Cosmos gateways that throttle reflection hit the same wall
// through a different door.
func DescriptorSourceForNode(mode, descriptorSetPath string, serverSource grpcurl.DescriptorSource) (grpcurl.DescriptorSource, error) {
	switch mode {
	case common.GrpcDescriptorSourceFile:
		if descriptorSetPath == "" {
			return nil, utils.LavaFormatError("descriptor-set-path is required for descriptor-source: file", nil)
		}
		return LoadProtoset(descriptorSetPath)

	case common.GrpcDescriptorSourceHybrid:
		if descriptorSetPath == "" {
			// Config validation rejects this, but a hybrid node with no protoset is
			// still meaningfully reflection-only rather than a hard failure.
			return serverSource, nil
		}
		fileSource, err := LoadProtoset(descriptorSetPath)
		if err != nil {
			// Hybrid tolerates an unusable protoset — reflection is the other half.
			// Degrading here is the whole point of the mode.
			utils.LavaFormatWarning("hybrid descriptor source: protoset unusable, falling back to reflection only", err,
				utils.LogAttr("descriptor-set-path", descriptorSetPath))
			return serverSource, nil
		}
		return compositeDescriptorSource{file: fileSource, server: serverSource}, nil

	case common.GrpcDescriptorSourceReflection, "":
		return serverSource, nil

	default:
		return nil, utils.LavaFormatError("invalid gRPC descriptor-source", nil,
			utils.LogAttr("descriptor-source", mode),
			utils.LogAttr("valid_options", fmt.Sprintf("%s, %s, %s",
				common.GrpcDescriptorSourceReflection,
				common.GrpcDescriptorSourceFile,
				common.GrpcDescriptorSourceHybrid)))
	}
}

// DescriptorSourceForGrpcConfig is the call-site convenience over
// DescriptorSourceForNode; a nil config means the reflection default.
func DescriptorSourceForGrpcConfig(grpcConfig *common.GrpcConfig, serverSource grpcurl.DescriptorSource) (grpcurl.DescriptorSource, error) {
	if grpcConfig == nil {
		return serverSource, nil
	}
	return DescriptorSourceForNode(grpcConfig.GetDescriptorSource(), grpcConfig.DescriptorSetPath, serverSource)
}
