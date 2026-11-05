package grpcproxy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type admissionStream struct {
	grpc.ServerStream
	ctx  context.Context
	read func() error
}

func (s *admissionStream) Context() context.Context  { return s.ctx }
func (s *admissionStream) RecvMsg(interface{}) error { return s.read() }

type admissionKey struct{}

func TestRequestObserverStartsBeforeDecodeAndKeepsFailure(t *testing.T) {
	failure := errors.New("decode refused")
	started, finished := 0, 0
	observer := func(ctx context.Context, _ string) (context.Context, func(error)) {
		started++
		return context.WithValue(ctx, admissionKey{}, true), func(err error) { finished++; require.ErrorIs(t, err, failure) }
	}
	handler := func(_ interface{}, stream grpc.ServerStream) error {
		require.Equal(t, 1, started)
		require.Equal(t, true, stream.Context().Value(admissionKey{}))
		return stream.RecvMsg(nil)
	}
	stream := &admissionStream{ctx: context.Background(), read: func() error { require.Equal(t, 1, started); return failure }}
	require.ErrorIs(t, observeProxyRequests(handler, observer)(nil, stream), failure)
	require.Equal(t, 1, finished)
}
