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
	mu             sync.Mutex
	writes         []webSocketMsgWithType
	writeDeadlines []time.Time
	readDeadline   bool
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

func (r *recordingFrameWriter) SetWriteDeadline(deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeDeadlines = append(r.writeDeadlines, deadline)
	return nil
}

func (r *recordingFrameWriter) countWrites(messageType int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, w := range r.writes {
		if w.messageType == messageType {
			n++
		}
	}
	return n
}

// TestWebsocketWriter_BoundsEveryWrite: the write deadline is the only bound on a
// client that stopped reading. Every frame the writer sends must carry one, set
// from the tunable, and a zero tunable must set none, since that is what the flag
// documents.
func TestWebsocketWriter_BoundsEveryWrite(t *testing.T) {
	previous := GetWebSocketWriteTimeout()
	defer SetWebSocketWriteTimeout(previous)

	const timeout = 250 * time.Millisecond
	SetWebSocketWriteTimeout(timeout)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	webSocketCtx, cancelWebSocketCtx := context.WithCancel(context.Background())
	defer cancelWebSocketCtx()

	conn := &recordingFrameWriter{}
	frames := make(chan webSocketMsgWithType)
	writerDone := make(chan struct{})
	go runWebsocketWriter(ctx, webSocketCtx, conn, frames, writerDone)

	before := time.Now()
	for _, payload := range []string{"one", "two"} {
		enqueueWebsocketFrame(ctx, webSocketCtx, writerDone, frames, webSocketMsgWithType{messageType: websocket.TextMessage, msg: []byte(payload)})
	}
	require.Eventually(t, func() bool { return conn.countWrites(websocket.TextMessage) == 2 }, 2*time.Second, 5*time.Millisecond)
	after := time.Now()

	conn.mu.Lock()
	deadlines := append([]time.Time(nil), conn.writeDeadlines...)
	conn.mu.Unlock()
	require.Len(t, deadlines, 2, "every frame must be written under a deadline")
	for _, deadline := range deadlines {
		require.False(t, deadline.Before(before.Add(timeout)), "the deadline must be the configured timeout past the write")
		require.False(t, deadline.After(after.Add(timeout)), "the deadline must be the configured timeout past the write")
	}

	// Zero disables the deadline.
	cancelWebSocketCtx()
	<-writerDone
	SetWebSocketWriteTimeout(0)
	webSocketCtx2, cancelWebSocketCtx2 := context.WithCancel(context.Background())
	defer cancelWebSocketCtx2()
	conn2 := &recordingFrameWriter{}
	frames2 := make(chan webSocketMsgWithType)
	writerDone2 := make(chan struct{})
	go runWebsocketWriter(ctx, webSocketCtx2, conn2, frames2, writerDone2)
	enqueueWebsocketFrame(ctx, webSocketCtx2, writerDone2, frames2, webSocketMsgWithType{messageType: websocket.TextMessage, msg: []byte("unbounded")})
	require.Eventually(t, func() bool { return conn2.countWrites(websocket.TextMessage) == 1 }, 2*time.Second, 5*time.Millisecond)
	conn2.mu.Lock()
	require.Empty(t, conn2.writeDeadlines, "a zero timeout must not set a deadline")
	conn2.mu.Unlock()
}

// TestWebsocketWriter_PingsOnlyAfterSilence: the keep-alive exists to keep a
// quiet connection from being reaped, so it fires once per interval of silence and
// not at all while frames are going out; every write resets the interval.
func TestWebsocketWriter_PingsOnlyAfterSilence(t *testing.T) {
	previous := GetWebSocketKeepAliveInterval()
	defer SetWebSocketKeepAliveInterval(previous)

	const interval = 100 * time.Millisecond
	SetWebSocketKeepAliveInterval(interval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	webSocketCtx, cancelWebSocketCtx := context.WithCancel(context.Background())
	defer cancelWebSocketCtx()

	conn := &recordingFrameWriter{}
	frames := make(chan webSocketMsgWithType)
	writerDone := make(chan struct{})
	go runWebsocketWriter(ctx, webSocketCtx, conn, frames, writerDone)

	// A busy connection: a frame every quarter interval for four intervals.
	busyUntil := time.Now().Add(4 * interval)
	for time.Now().Before(busyUntil) {
		enqueueWebsocketFrame(ctx, webSocketCtx, writerDone, frames, webSocketMsgWithType{messageType: websocket.TextMessage, msg: []byte("payload")})
		time.Sleep(interval / 4)
	}
	require.Equal(t, 0, conn.countWrites(websocket.PingMessage), "a connection that is writing must not be pinged")

	// Silence: a ping within the interval, then one per interval.
	require.Eventually(t, func() bool { return conn.countWrites(websocket.PingMessage) >= 1 }, 2*interval, 5*time.Millisecond,
		"a quiet connection must be pinged within one interval")
	require.Eventually(t, func() bool { return conn.countWrites(websocket.PingMessage) >= 3 }, 5*interval, 5*time.Millisecond,
		"pings must keep coming while the connection stays quiet")
}

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
