package rpcsmartrouter

// The relay_end event of each recent request, kept by GUID and read back by GET /debug/relay-end
// (MAG-4899), so a test asserts on the router's record of one request instead of searching the log.
//
// The record is the event's own object, rendered by the same MarshalZerologObject the log line is,
// so the two cannot drift: docs/RELAY-END-EVENT.md is the contract of both.
//
// Keeping records is DEBUG-MODE ONLY. The store is installed by enableDebugRecorders, which Start
// calls when --debug-address is set, so a production router pays one atomic nil load per relay and
// keeps nothing. It is bounded in records and in bytes (relayEndStoreByteBudget).

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/magma-Devs/smart-router/utils"
	zerolog "github.com/rs/zerolog"
)

// debugLogBufferCapacity is the number of lines the /debug/logs ring holds, and the most records
// the relay_end store holds.
const debugLogBufferCapacity = 50000

// relayEndStoreByteBudget bounds the bytes of the records the relay_end store holds. A count alone
// does not bound its memory, because a record's size follows its attempts: about 400 B for a read one
// provider answered, about 1.2 KB for four failed attempts, more with long node messages. 50 000
// records of failed relays came to 60-90 MiB, on top of the /debug/logs ring, on routers that run
// --debug-address with a 512 MiB limit (k3d). 16 MiB still holds tens of thousands of the newest
// records, far more than a test reads back.
const relayEndStoreByteBudget = 16 << 20

// relayEndStore keeps the newest records, one per GUID, and drops the oldest GUID when it holds
// capacity records or the records' bytes would pass byteBudget. Written from every relay's goroutine
// and read from the debug HTTP handler, so every access takes the lock.
type relayEndStore struct {
	mu         sync.Mutex
	records    map[string]*relayEndRecord
	order      []string // the GUIDs held, oldest first
	capacity   int
	byteBudget int
	bytes      int // the bytes of every record's line
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

// enableRelayEndStore installs a store of at most capacity records and byteBudget bytes of them. Its
// map and order grow with what it holds, so an idle debug router pays nothing up front.
func enableRelayEndStore(capacity, byteBudget int) {
	relayEndRecorder.Store(&relayEndStore{
		records:    make(map[string]*relayEndRecord),
		capacity:   capacity,
		byteBudget: byteBudget,
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
		store.bytes += len(line) - len(record.line)
		record.line = line
		record.writes++
		store.dropOldestWhileOver(false, 0, end.guid)
		return
	}
	store.dropOldestWhileOver(true, len(line), "")
	store.order = append(store.order, end.guid)
	store.records[end.guid] = &relayEndRecord{line: line, writes: 1}
	store.bytes += len(line)
}

// dropOldestWhileOver drops the oldest GUID while the store, with incoming more bytes and, when
// adding, one more record, would pass its bounds. It never drops keep, the GUID just rewritten, so a
// rewrite cannot lose the record it counts. A new record is added after it returns, so one record
// larger than the whole budget is still kept, alone. Called with the lock held.
func (store *relayEndStore) dropOldestWhileOver(adding bool, incoming int, keep string) {
	added := 0
	if adding {
		added = 1
	}
	for len(store.order) > 0 && store.order[0] != keep &&
		(len(store.order)+added > store.capacity || store.bytes+incoming > store.byteBudget) {
		oldest := store.order[0]
		store.bytes -= len(store.records[oldest].line)
		delete(store.records, oldest)
		store.order = store.order[1:]
	}
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
