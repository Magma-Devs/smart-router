package dyncodec

import (
	"fmt"
	"strings"
	"sync"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// This is the ProtoFileRegistry half of descriptor-source selection. The grpcurl
// DescriptorSource half lives in rpcInterfaceMessages/fileDescriptorSource.go.
//
// Both exist because the router resolves descriptors through two unrelated
// interfaces: grpcurl.DescriptorSource drives request/response marshalling, while
// ProtoFileRegistry backs GrpcMessage.dynamicResolve — i.e. GetParams, i.e. block
// parsing on binary-proto requests. Wiring only the first leaves a node that does
// not serve reflection booting successfully and then failing at parse time.

// fileRegistryCache memoizes parsed descriptor-set registries by path, for the same
// reason the protoset cache exists: the file is static config and parsing it is not
// free. Entries are shared across every proxy that names the same path.
var fileRegistryCache sync.Map // map[string]*fileRegistryEntry

type fileRegistryEntry struct {
	once     sync.Once
	registry *FileDescriptorSetRegistry
	err      error
}

// loadCachedFileRegistry parses a descriptor set at most once per path per process.
func loadCachedFileRegistry(path string) (*FileDescriptorSetRegistry, error) {
	cached, _ := fileRegistryCache.LoadOrStore(path, &fileRegistryEntry{})
	entry, ok := cached.(*fileRegistryEntry)
	if !ok {
		return nil, utils.LavaFormatError("corrupt descriptor set cache entry", nil,
			utils.LogAttr("descriptor-set-path", path))
	}

	entry.once.Do(func() {
		entry.registry, entry.err = NewFileDescriptorSetRegistryFromPath(path)
	})

	return entry.registry, entry.err
}

// ResetFileRegistryCacheForTest drops every memoized registry. Production never
// calls this; tests that rewrite a descriptor set at a reused temp path must.
func ResetFileRegistryCacheForTest() {
	fileRegistryCache.Range(func(key, _ any) bool {
		fileRegistryCache.Delete(key)
		return true
	})
}

// sharedFileRegistry adapts a cached registry to ProtoFileRegistry while dropping
// Close on the floor.
//
// FileDescriptorSetRegistry.Close latches the registry shut and every subsequent
// lookup returns "registry is closed". Since one parsed registry is now shared by
// every proxy naming that path, honouring Close from any single consumer would
// silently break the others. The underlying data is immutable and lives for the
// process, so there is nothing to release.
type sharedFileRegistry struct {
	*FileDescriptorSetRegistry
}

func (sharedFileRegistry) Close() error { return nil }

// compositeProtoFileRegistry resolves against a descriptor set first and falls back
// to reflection — the ProtoFileRegistry form of "hybrid" mode. File-first for the
// same reason as the grpcurl side: hybrid exists precisely for nodes whose
// reflection is missing, partial, or throttled.
type compositeProtoFileRegistry struct {
	file   ProtoFileRegistry
	server ProtoFileRegistry
}

var _ ProtoFileRegistry = (*compositeProtoFileRegistry)(nil)

// exactSymbolLookup is the strict half of a descriptor-set registry: does it declare
// this symbol itself, as opposed to merely declaring one of its ancestors.
//
// FileDescriptorSetRegistry.ProtoFileContainingSymbol walks up the parent chain —
// "pkg.Query.Ping" resolves through "pkg.Query" — and on a miss returns the
// ancestor's file with a nil error. That is harmless in "file" mode, where the
// lookup was going to fail either way, but in hybrid the nil error is precisely what
// decides whether reflection gets asked. An rpc the node gained after the protoset
// was cut resolves to the stale file, reflection is never consulted, and the
// operator sees "not found" at the one point the mode exists to cover.
type exactSymbolLookup interface {
	HasSymbol(name string) bool
}

// fileHalfDeclares reports whether the descriptor-set half declares name itself. A
// half that cannot answer strictly is taken at its word.
func (c *compositeProtoFileRegistry) fileHalfDeclares(name protoreflect.FullName) bool {
	strict, ok := c.file.(exactSymbolLookup)
	if !ok {
		return true
	}
	// ProtoFileContainingSymbol tolerates a leading dot; the symbol index does not.
	return strict.HasSymbol(strings.TrimPrefix(string(name), "."))
}

func (c *compositeProtoFileRegistry) ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error) {
	file, fileErr := c.file.ProtoFileByPath(path)
	if fileErr == nil {
		return file, nil
	}

	file, serverErr := c.server.ProtoFileByPath(path)
	if serverErr == nil {
		return file, nil
	}

	return nil, utils.LavaFormatError("proto file not found in descriptor set or via reflection", nil,
		utils.LogAttr("path", path),
		utils.LogAttr("descriptor_set_error", fileErr),
		utils.LogAttr("reflection_error", serverErr))
}

func (c *compositeProtoFileRegistry) ProtoFileContainingSymbol(name protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error) {
	file, fileErr := c.file.ProtoFileContainingSymbol(name)
	if fileErr == nil && c.fileHalfDeclares(name) {
		return file, nil
	}
	if fileErr == nil {
		// An ancestor matched but the symbol itself is absent. Treating that as a hit
		// is what made hybrid's fallback unreachable; treat it as the miss it is.
		fileErr = fmt.Errorf("descriptor set declares an ancestor of %s but not the symbol itself", name)
	}

	file, serverErr := c.server.ProtoFileContainingSymbol(name)
	if serverErr == nil {
		return file, nil
	}

	return nil, utils.LavaFormatError("symbol not found in descriptor set or via reflection", nil,
		utils.LogAttr("symbol", string(name)),
		utils.LogAttr("descriptor_set_error", fileErr),
		utils.LogAttr("reflection_error", serverErr))
}

// Close closes only the reflection half; the descriptor-set half is a shared
// cached registry that must outlive any one consumer.
func (c *compositeProtoFileRegistry) Close() error {
	fileErr := c.file.Close()
	serverErr := c.server.Close()
	if serverErr != nil {
		return serverErr
	}
	return fileErr
}

// ProtoFileRegistryForNode picks the ProtoFileRegistry implied by a node's
// grpc-config. serverRemote is the reflection-backed registry the caller already
// built; in "file" mode it is never consulted, and is closed here so the caller
// does not leak it.
func ProtoFileRegistryForNode(mode, descriptorSetPath string, serverRemote ProtoFileRegistry) (ProtoFileRegistry, error) {
	switch mode {
	case common.GrpcDescriptorSourceFile:
		if descriptorSetPath == "" {
			return nil, utils.LavaFormatError("descriptor-set-path is required for descriptor-source: file", nil)
		}
		registry, err := loadCachedFileRegistry(descriptorSetPath)
		if err != nil {
			return nil, err
		}
		if serverRemote != nil {
			// Nothing will consult reflection in this mode; release it rather than
			// hold an idle handle for the life of the proxy.
			_ = serverRemote.Close()
		}
		return sharedFileRegistry{FileDescriptorSetRegistry: registry}, nil

	case common.GrpcDescriptorSourceHybrid:
		if descriptorSetPath == "" {
			return serverRemote, nil
		}
		registry, err := loadCachedFileRegistry(descriptorSetPath)
		if err != nil {
			utils.LavaFormatWarning("hybrid descriptor source: descriptor set unusable, falling back to reflection only", err,
				utils.LogAttr("descriptor-set-path", descriptorSetPath))
			return serverRemote, nil
		}
		return &compositeProtoFileRegistry{
			file:   sharedFileRegistry{FileDescriptorSetRegistry: registry},
			server: serverRemote,
		}, nil

	case common.GrpcDescriptorSourceReflection, "":
		return serverRemote, nil

	default:
		return nil, utils.LavaFormatError("invalid gRPC descriptor-source", nil,
			utils.LogAttr("descriptor-source", mode),
			utils.LogAttr("valid_options", fmt.Sprintf("%s, %s, %s",
				common.GrpcDescriptorSourceReflection,
				common.GrpcDescriptorSourceFile,
				common.GrpcDescriptorSourceHybrid)))
	}
}

// ProtoFileRegistryForGrpcConfig is the call-site convenience over
// ProtoFileRegistryForNode; a nil config means the reflection default. It mirrors
// rpcInterfaceMessages.DescriptorSourceForGrpcConfig on the other half.
func ProtoFileRegistryForGrpcConfig(grpcConfig *common.GrpcConfig, serverRemote ProtoFileRegistry) (ProtoFileRegistry, error) {
	if grpcConfig == nil {
		return serverRemote, nil
	}
	return ProtoFileRegistryForNode(grpcConfig.GetDescriptorSource(), grpcConfig.DescriptorSetPath, serverRemote)
}
