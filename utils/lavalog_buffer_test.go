package utils

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	zerolog "github.com/rs/zerolog"
)

// resetDebugBuffer restores the debug-buffer package state to its disabled
// default. lavalog uses package-level singletons; tests must leave the buffer
// disabled so they don't leak into one another or other package tests.
func resetDebugBuffer() {
	debugRingMu.Lock()
	debugRing = nil
	debugRingMu.Unlock()
	debugBufferLogger = zerolog.New(io.Discard).Level(zerolog.Disabled)
}

func TestDebugRingWriter_EvictsOldestAtCapacity(t *testing.T) {
	const cap = 4
	ring := newDebugRingWriter(cap)

	// Write cap+N records; only the newest `cap` must survive.
	const extra = 6
	for i := 0; i < cap+extra; i++ {
		if _, err := ring.Write([]byte{byte('A' + i)}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	snap := ring.snapshot()
	if len(snap) != cap {
		t.Fatalf("snapshot len = %d, want %d", len(snap), cap)
	}
	// Newest cap records are indices [extra, cap+extra) → bytes 'A'+extra ...
	for idx, rec := range snap {
		want := byte('A' + extra + idx)
		if len(rec) != 1 || rec[0] != want {
			t.Fatalf("snapshot[%d] = %q, want %q", idx, rec, []byte{want})
		}
	}
}

func TestDebugRingWriter_CopiesInput(t *testing.T) {
	ring := newDebugRingWriter(2)
	p := []byte("hello")
	ring.Write(p)
	// Mutate the caller's slice the way zerolog reuses its buffer.
	for i := range p {
		p[i] = 'X'
	}
	snap := ring.snapshot()
	if string(snap[0]) != "hello" {
		t.Fatalf("ring did not copy input: got %q", snap[0])
	}
}

func TestReadDebugLogBuffer_RequestIDFilter(t *testing.T) {
	resetDebugBuffer()
	defer resetDebugBuffer()

	EnableDebugLogBuffer(100)
	// defaultGlobalLogLevel is DebugLevel by default → Info passes the gate.
	LavaFormatInfo("hello target", LogAttr(KEY_REQUEST_ID, "req-123"))
	LavaFormatInfo("hello other", LogAttr(KEY_REQUEST_ID, "req-999"))

	got := ReadDebugLogBuffer("req-123", time.Time{}, time.Time{}, 0)
	if len(got) != 1 {
		t.Fatalf("request_id=req-123 returned %d records, want 1: %s", len(got), dump(got))
	}
	assertRecordHasRequestID(t, got[0], "req-123")

	none := ReadDebugLogBuffer("does-not-exist", time.Time{}, time.Time{}, 0)
	if len(none) != 0 {
		t.Fatalf("request_id=does-not-exist returned %d records, want 0", len(none))
	}
}

func TestReadDebugLogBuffer_TimeWindowFilter(t *testing.T) {
	resetDebugBuffer()
	defer resetDebugBuffer()

	EnableDebugLogBuffer(100)
	before := time.Now()
	LavaFormatInfo("within window")
	after := time.Now()

	// A window that brackets the record keeps it.
	within := ReadDebugLogBuffer("", before.Add(-time.Second), after.Add(time.Second), 0)
	if len(within) == 0 {
		t.Fatalf("expected at least 1 record within [%v,%v]", before, after)
	}

	// A window entirely in the past excludes it.
	past := ReadDebugLogBuffer("", before.Add(-2*time.Hour), before.Add(-time.Hour), 0)
	if len(past) != 0 {
		t.Fatalf("expected 0 records in past window, got %d", len(past))
	}

	// A window entirely in the future excludes it.
	future := ReadDebugLogBuffer("", after.Add(time.Hour), after.Add(2*time.Hour), 0)
	if len(future) != 0 {
		t.Fatalf("expected 0 records in future window, got %d", len(future))
	}
}

func TestReadDebugLogBuffer_LimitKeepsTail(t *testing.T) {
	resetDebugBuffer()
	defer resetDebugBuffer()

	EnableDebugLogBuffer(100)
	for i := 0; i < 10; i++ {
		LavaFormatInfo("msg", LogAttr("i", i))
	}
	got := ReadDebugLogBuffer("", time.Time{}, time.Time{}, 3)
	if len(got) != 3 {
		t.Fatalf("limit=3 returned %d records, want 3", len(got))
	}
	// Tail → last record must be i=9.
	assertRecordField(t, got[2], "i", "9")
}

func TestReadDebugLogBuffer_DisabledByDefault(t *testing.T) {
	resetDebugBuffer()
	defer resetDebugBuffer()

	// No EnableDebugLogBuffer call → buffer is disabled.
	LavaFormatInfo("should not be buffered", LogAttr(KEY_REQUEST_ID, "req-abc"))
	got := ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0)
	if len(got) != 0 {
		t.Fatalf("buffer disabled by default but returned %d records", len(got))
	}
}

func TestClearDebugLogBuffer(t *testing.T) {
	resetDebugBuffer()
	defer resetDebugBuffer()

	EnableDebugLogBuffer(100)
	LavaFormatInfo("one")
	LavaFormatInfo("two")
	if got := ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0); len(got) != 2 {
		t.Fatalf("pre-clear: %d records, want 2", len(got))
	}
	ClearDebugLogBuffer()
	if got := ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0); len(got) != 0 {
		t.Fatalf("post-clear: %d records, want 0", len(got))
	}
}

// --- helpers ---

func assertRecordHasRequestID(t *testing.T, rec []byte, want string) {
	t.Helper()
	assertRecordField(t, rec, KEY_REQUEST_ID, want)
}

func assertRecordField(t *testing.T, rec []byte, key, want string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rec, &fields); err != nil {
		t.Fatalf("record is not valid JSON (%v): %s", err, rec)
	}
	raw, ok := fields[key]
	if !ok {
		t.Fatalf("record missing %q field: %s", key, rec)
	}
	var got string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("field %q not a string (%v): %s", key, err, rec)
	}
	if got != want {
		t.Fatalf("field %q = %q, want %q", key, got, want)
	}
}

func dump(recs [][]byte) string {
	out := ""
	for _, r := range recs {
		out += string(r)
	}
	return out
}
