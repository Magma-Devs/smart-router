package dyncodec

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func NewRegistry(remote ProtoFileRegistry) *Registry {
	return &Registry{
		remote:    remote,
		prefFiles: new(protoregistry.Files),
		prefTypes: new(protoregistry.Types),
	}
}

var (
	_ protodesc.Resolver                  = (*Registry)(nil)
	_ protoregistry.ExtensionTypeResolver = (*Registry)(nil)
	_ protoregistry.MessageTypeResolver   = (*Registry)(nil)
)

type ProtoFileRegistry interface {
	ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error)
	ProtoFileContainingSymbol(name protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error)
	Close() error
}

type Registry struct {
	remote ProtoFileRegistry

	// mu guards prefFiles and prefTypes, which every relay on the chain shares. It
	// is never held across a remote fetch or protodesc.New, since both resolve back
	// through the registry. When two resolutions of one file or type race, the first
	// registered is kept and returned to both.
	mu        sync.RWMutex
	prefFiles *protoregistry.Files
	prefTypes *protoregistry.Types
}

// lookup answers from the registry's own files and types under the read lock and, when they do
// not hold the answer, resolves it with resolve. Any error other than NotFound is returned as is.
func lookup[T any](r *Registry, held func() (T, error), resolve func() (T, error)) (T, error) {
	r.mu.RLock()
	v, err := held()
	r.mu.RUnlock()
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		var zero T
		return zero, err
	}
	return resolve()
}

func (r *Registry) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	return lookup(r, func() (protoreflect.ExtensionType, error) { return r.prefTypes.FindExtensionByName(field) },
		func() (protoreflect.ExtensionType, error) {
			xd, err := r.FindDescriptorByName(field)
			if err != nil {
				return nil, err
			}
			xdExtensionDescriptor, ok := xd.(protoreflect.ExtensionDescriptor)
			if !ok {
				return nil, utils.LavaFormatError("Failed converting xd.(protoreflect.ExtensionDescriptor)", nil, utils.Attribute{Key: "xd", Value: xd})
			}
			return r.registerExtension(dynamicpb.NewExtensionType(xdExtensionDescriptor))
		})
}

func (r *Registry) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	return nil, fmt.Errorf("not supported")
}

func (r *Registry) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	return lookup(r, func() (protoreflect.MessageType, error) { return r.prefTypes.FindMessageByName(message) },
		func() (protoreflect.MessageType, error) { return r.resolveMessage(message) })
}

func (r *Registry) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	return lookup(r, func() (protoreflect.MessageType, error) { return r.prefTypes.FindMessageByURL(url) },
		func() (protoreflect.MessageType, error) { return r.resolveMessage(fullNameFromURL(url)) })
}

// resolveMessage builds a dynamic message type from message's descriptor and registers it.
func (r *Registry) resolveMessage(message protoreflect.FullName) (protoreflect.MessageType, error) {
	md, err := r.FindDescriptorByName(message)
	if err != nil {
		return nil, err
	}
	messageDescriptor, ok := md.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, utils.LavaFormatError("Failed converting md.(protoreflect.MessageDescriptor)", nil, utils.Attribute{Key: "md", Value: md})
	}
	return r.registerMessage(dynamicpb.NewMessageType(messageDescriptor))
}

func (r *Registry) FindFileByPath(s string) (protoreflect.FileDescriptor, error) {
	return lookup(r, func() (protoreflect.FileDescriptor, error) { return r.prefFiles.FindFileByPath(s) },
		func() (protoreflect.FileDescriptor, error) {
			fd, err := r.newFile(r.remote.ProtoFileByPath(s))
			if err != nil {
				return nil, err
			}
			return r.registerFile(fd)
		})
}

func (r *Registry) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return lookup(r, func() (protoreflect.Descriptor, error) { return r.prefFiles.FindDescriptorByName(name) },
		func() (protoreflect.Descriptor, error) {
			fd, err := r.newFile(r.remote.ProtoFileContainingSymbol(name))
			if err != nil {
				return nil, err
			}
			if _, err := r.registerFile(fd); err != nil {
				return nil, err
			}
			r.mu.RLock()
			defer r.mu.RUnlock()
			return r.prefFiles.FindDescriptorByName(name)
		})
}

// newFile builds a file descriptor from a fetched proto, resolving its imports through the
// registry. It takes the fetch's own result so a failed fetch passes straight through.
func (r *Registry) newFile(dpb *descriptorpb.FileDescriptorProto, err error) (protoreflect.FileDescriptor, error) {
	if err != nil {
		return nil, err
	}
	return protodesc.FileOptions{
		AllowUnresolvable: true,
	}.New(dpb, r)
}

// registerFile registers fd unless a resolution that raced it registered the same
// path first, and returns whichever one the registry holds.
func (r *Registry) registerFile(fd protoreflect.FileDescriptor) (protoreflect.FileDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if held, err := r.prefFiles.FindFileByPath(fd.Path()); err == nil {
		return held, nil
	}
	if err := r.prefFiles.RegisterFile(fd); err != nil {
		return nil, err
	}
	return fd, nil
}

// registerMessage registers mt unless a resolution that raced it registered the
// same message first, and returns whichever one the registry holds.
func (r *Registry) registerMessage(mt protoreflect.MessageType) (protoreflect.MessageType, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if held, err := r.prefTypes.FindMessageByName(mt.Descriptor().FullName()); err == nil {
		return held, nil
	}
	return mt, r.prefTypes.RegisterMessage(mt)
}

// registerExtension registers xt unless a resolution that raced it registered the
// same extension first, and returns whichever one the registry holds.
func (r *Registry) registerExtension(xt protoreflect.ExtensionType) (protoreflect.ExtensionType, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if held, err := r.prefTypes.FindExtensionByName(xt.TypeDescriptor().FullName()); err == nil {
		return held, nil
	}
	return xt, r.prefTypes.RegisterExtension(xt)
}

func (r *Registry) Save() (*descriptorpb.FileDescriptorSet, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := &descriptorpb.FileDescriptorSet{File: make([]*descriptorpb.FileDescriptorProto, 0, r.prefFiles.NumFiles())}
	var err error
	r.prefFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
		return true
	})

	return set, err
}

func (r *Registry) Remote() ProtoFileRegistry {
	return r.remote
}

// fullNameFromURL returns protoreflect.FullName from proto.Messages' typeURL
func fullNameFromURL(typeURL string) protoreflect.FullName {
	message := protoreflect.FullName(typeURL)
	if i := strings.LastIndexByte(typeURL, '/'); i >= 0 {
		message = message[i+len("/"):]
	}

	return message
}
