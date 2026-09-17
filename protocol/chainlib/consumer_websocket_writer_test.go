package chainlib

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/websocket/v2"
	"github.com/stretchr/testify/require"
)

// failingFrameWriter fails every write and records whether the read loop was woken.
type failingFrameWriter struct {
	mu           sync.Mutex
	writes       int
	readDeadline bool
}

func (f *failingFrameWriter) WriteMessage(int, []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	return errors.New("broken pipe")
}

func (f *failingFrameWriter) SetReadDeadline(time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readDeadline = true
	return nil
}

func (f *failingFrameWriter) SetWriteDeadline(time.Time) error { return nil }

// TestWebsocketWriterFailure_UnblocksSenders is the regression for MAG-3722: when the
// writer goroutine died on a failed write, every later frame sender blocked forever, and
// the read loop's own error path is one of them, so the handler never returned and the
// connection's buffers, goroutines and per-IP limiter slot were held for good.
func TestWebsocketWriterFailure_UnblocksSenders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	webSocketCtx, cancelWebSocketCtx := context.WithCancel(context.Background())
	defer cancelWebSocketCtx()

	conn := &failingFrameWriter{}
	frames := make(chan webSocketMsgWithType)
	writerDone := make(chan struct{})
	go runWebsocketWriter(ctx, webSocketCtx, conn, frames, writerDone)

	// The first frame reaches the writer, whose write fails and ends it.
	enqueueWebsocketFrame(ctx, webSocketCtx, writerDone, frames, webSocketMsgWithType{messageType: 1, msg: []byte("a")})
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("a failed write must end the writer goroutine")
	}

	conn.mu.Lock()
	require.Equal(t, 1, conn.writes)
	require.True(t, conn.readDeadline, "a failed write must wake the read loop so the handler can return")
	conn.mu.Unlock()

	// Every later sender, the read loop's error reply included, must return at once.
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		enqueueWebsocketFrame(ctx, webSocketCtx, writerDone, frames, webSocketMsgWithType{messageType: 1, msg: []byte("b")})
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("a frame sent after the writer died must not block")
	}
}

// recordingFrameWriter is the mirror of failingFrameWriter: every write succeeds,
// so a test can assert on what actually reached the connection.
type recordingFrameWriter struct {
	mu           sync.Mutex
	writes       []webSocketMsgWithType
	readDeadline bool
}

func (r *recordingFrameWriter) WriteMessage(messageType int, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, webSocketMsgWithType{messageType: messageType, msg: data})
	return nil
}

func (r *recordingFrameWriter) SetReadDeadline(time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readDeadline = true
	return nil
}

func (r *recordingFrameWriter) SetWriteDeadline(time.Time) error { return nil }

// TestWebsocketWriter_FinalFrameTearsDownConnection: the idle reaper hands the writer a
// close frame and returns, so the writer is the only thing left that can end the
// connection. Waiting for the client to close on that frame is not enough — one that
// ignores it used to be reaped by the proxy in front of the router, and the keep-alive
// ping means the connection is never silent enough for that to happen any more.
func TestWebsocketWriter_FinalFrameTearsDownConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	webSocketCtx, cancelWebSocketCtx := context.WithCancel(context.Background())
	defer cancelWebSocketCtx()

	conn := &recordingFrameWriter{}
	frames := make(chan webSocketMsgWithType)
	writerDone := make(chan struct{})
	go runWebsocketWriter(ctx, webSocketCtx, conn, frames, writerDone)

	// A regular frame leaves the writer serving the connection...
	enqueueWebsocketFrame(ctx, webSocketCtx, writerDone, frames, webSocketMsgWithType{messageType: websocket.TextMessage, msg: []byte("subscription payload")})
	select {
	case <-writerDone:
		t.Fatal("a regular frame must not end the writer")
	case <-time.After(100 * time.Millisecond):
	}

	// ...a final one ends it, after putting the frame on the wire.
	enqueueWebsocketFrame(ctx, webSocketCtx, writerDone, frames, webSocketMsgWithType{
		messageType: websocket.CloseMessage,
		msg:         websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Connection idle for too long"),
		final:       true,
	})
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("a final frame must end the writer goroutine")
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	require.Len(t, conn.writes, 2, "the final frame must reach the connection before teardown")
	require.Equal(t, websocket.CloseMessage, conn.writes[1].messageType)
	require.True(t, conn.readDeadline, "a final frame must wake the read loop so the handler can return")
}

// TestWebsocketWriter_ExitsOnConnectionContext keeps the normal teardown path honest:
// cancelling the per-connection context ends the writer without touching the conn.
func TestWebsocketWriter_ExitsOnConnectionContext(t *testing.T) {
	webSocketCtx, cancelWebSocketCtx := context.WithCancel(context.Background())
	conn := &failingFrameWriter{}
	writerDone := make(chan struct{})
	go runWebsocketWriter(context.Background(), webSocketCtx, conn, make(chan webSocketMsgWithType), writerDone)

	cancelWebSocketCtx()
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the writer must end when the connection context is cancelled")
	}
	conn.mu.Lock()
	require.Equal(t, 0, conn.writes)
	conn.mu.Unlock()
}
