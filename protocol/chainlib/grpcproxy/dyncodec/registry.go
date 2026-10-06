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

func (r *Registry) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	// try in types
	r.mu.RLock()
	xt, err := r.prefTypes.FindExtensionByName(field)
	r.mu.RUnlock()
	if err == nil {
		return xt, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}
	// not found try in files
	xd, err := r.FindDescriptorByName(field)
	if err != nil {
		return nil, err
	}

	xdExtensionDescriptor, ok := xd.(protoreflect.ExtensionDescriptor)
	if !ok {
		return nil, utils.LavaFormatError("Failed converting xd.(protoreflect.ExtensionDescriptor)", nil, utils.Attribute{Key: "xd", Value: xd})
	}

	return r.registerExtension(dynamicpb.NewExtensionType(xdExtensionDescriptor))
}

func (r *Registry) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	return nil, fmt.Errorf("not supported")
}

func (r *Registry) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	r.mu.RLock()
	mt, err := r.prefTypes.FindMessageByName(message)
	r.mu.RUnlock()
	if err == nil {
		return mt, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}

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

func (r *Registry) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	r.mu.RLock()
	mt, err := r.prefTypes.FindMessageByURL(url)
	r.mu.RUnlock()
	if err == nil {
		return mt, err
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}

	message := fullNameFromURL(url)

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
	r.mu.RLock()
	fd, err := r.prefFiles.FindFileByPath(s)
	r.mu.RUnlock()
	if err == nil {
		return fd, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}

	dpb, err := r.remote.ProtoFileByPath(s)
	if err != nil {
		return nil, err
	}

	fd, err = protodesc.FileOptions{
		AllowUnresolvable: true,
	}.New(dpb, r)
	if err != nil {
		return nil, err
	}

	return r.registerFile(fd)
}

func (r *Registry) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	r.mu.RLock()
	desc, err := r.prefFiles.FindDescriptorByName(name)
	r.mu.RUnlock()
	if err == nil {
		return desc, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}

	dpb, err := r.remote.ProtoFileContainingSymbol(name)
	if err != nil {
		return nil, err
	}
	fd, err := protodesc.FileOptions{
		AllowUnresolvable: true,
	}.New(dpb, r)
	if err != nil {
		return nil, err
	}

	if _, err := r.registerFile(fd); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.prefFiles.FindDescriptorByName(name)
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
