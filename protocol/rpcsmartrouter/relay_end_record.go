package rpcsmartrouter

// The relay_end event of each recent request, kept by GUID and read back by GET /debug/relay-end
// (MAG-4899), so a test asserts on the router's record of one request instead of searching the log.
//
// The record is the event's own object, rendered by the same MarshalZerologObject the log line is,
// so the two cannot drift: docs/RELAY-END-EVENT.md is the contract of both.
//
// Keeping records is DEBUG-MODE ONLY. The store is installed by enableRelayEndStore, called from
// Start next to utils.EnableDebugLogBuffer when --debug-address is set, so a production router pays
// one atomic nil load per relay and keeps nothing.

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/magma-Devs/smart-router/utils"
	zerolog "github.com/rs/zerolog"
)

// debugLogBufferCapacity is the number of lines the /debug/logs ring holds. The relay_end store
// holds as many records: every relay writes its relay_end into that ring too, so the store keeps
// at least every record /debug/logs could still return.
const debugLogBufferCapacity = 50000

// relayEndStore keeps the newest records, one per GUID, and drops the oldest GUID at capacity.
// Written from every relay's goroutine and read from the debug HTTP handler, so every access takes
// the lock.
type relayEndStore struct {
	mu       sync.Mutex
	records  map[string]*relayEndRecord
	order    []string // the GUIDs held, oldest first
	capacity int
}

type relayEndRecord struct {
	line []byte // the last event written with this GUID, as the log line carries it
	// writes counts the events written with this GUID while the store held it. A relay writes one,
	// so a count above 1 is the router breaking that contract, and the reader must see it.
	writes int
}

// relayEndRecorder holds the live store, or nil when keeping records is off (the production
// default). An atomic pointer because enableRelayEndStore runs during Start while relays may
// already be ending.
var relayEndRecorder atomic.Pointer[relayEndStore]

func enableRelayEndStore(capacity int) {
	relayEndRecorder.Store(&relayEndStore{
		records:  make(map[string]*relayEndRecord, capacity),
		order:    make([]string, 0, capacity),
		capacity: capacity,
	})
}

// emitRelayEnd writes the relay's relay_end event and keeps it for GET /debug/relay-end.
func emitRelayEnd(end *relayEnd) {
	utils.LogEvent(relayEndEvent, end)
	recordRelayEnd(end)
}

// recordRelayEnd keeps end under its GUID, or does nothing when keeping records is off. An event
// without a GUID is not kept: no reply names it, so nothing could ask for it.
func recordRelayEnd(end *relayEnd) {
	store := relayEndRecorder.Load()
	if store == nil || end.guid == "" {
		return
	}
	line := relayEndLine(end)
	store.mu.Lock()
	defer store.mu.Unlock()
	if record, held := store.records[end.guid]; held {
		record.line = line
		record.writes++
		return
	}
	if len(store.order) >= store.capacity {
		delete(store.records, store.order[0])
		store.order = store.order[1:]
	}
	store.order = append(store.order, end.guid)
	store.records[end.guid] = &relayEndRecord{line: line, writes: 1}
}

// readRelayEnd returns the record kept for guid and how many events were written with it; writes is
// 0 when the store does not hold guid. enabled is false when no store is installed, which the caller
// must tell apart from a GUID it does not hold.
func readRelayEnd(guid string) (line []byte, writes int, enabled bool) {
	store := relayEndRecorder.Load()
	if store == nil {
		return nil, 0, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, held := store.records[guid]
	if !held {
		return nil, 0, true
	}
	return record.line, record.writes, true
}

// relayEndLine renders end as utils.LogEvent writes it: the event's fields at INFO, with the time
// it was kept and the event's name as the message.
func relayEndLine(end *relayEnd) []byte {
	var line bytes.Buffer
	logger := zerolog.New(&line).With().Timestamp().Logger()
	logger.Info().EmbedObject(end).Msg(relayEndEvent)
	return bytes.TrimSuffix(line.Bytes(), []byte("\n"))
}
