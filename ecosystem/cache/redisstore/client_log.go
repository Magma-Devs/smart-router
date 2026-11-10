package redisstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/redis/go-redis/v9"
)

// clientLogInterval is how often one kind of failure line from go-redis is
// printed. An outage repeats the same line on every attempt of every pod, so
// the lines in between are counted and the count rides on the next one.
const clientLogInterval = 10 * time.Second

const clientFailureMessage = "resp-cache client reported a failure"

// clientStateChange is one go-redis line that records a change of state: the
// message it is logged under and the attribute each argument becomes.
type clientStateChange struct {
	message string
	attrs   []string
}

// clientStateChanges is keyed by go-redis's format string, the only identity
// a line has. These are the sole record of a primary switch, so they are
// logged at info and never suppressed. A format the library rewords falls
// through to the failure path, which TestClientLogBridge catches.
var clientStateChanges = map[string]clientStateChange{
	"sentinel: new master=%q addr=%q":                    {"resp-cache client follows a new primary", []string{"master", "address"}},
	"sentinel: selected addr=%s masterAddr=%s":           {"resp-cache client attached to a sentinel", []string{"sentinel", "primary"}},
	"sentinel: discovered new sentinel=%q for master=%q": {"resp-cache client discovered a sentinel", []string{"sentinel", "master"}},
}

// clientLogger is go-redis's process-wide logger, which is why its lines name
// no store: the library does not say which client a line came from.
type clientLogger struct {
	now func() time.Time

	mu    sync.Mutex
	kinds map[string]*clientLogKind
}

type clientLogKind struct {
	printedAt  time.Time
	suppressed int
}

var installClientLogger = sync.OnceFunc(func() {
	redis.SetLogger(&clientLogger{now: time.Now})
})

func (l *clientLogger) Printf(_ context.Context, format string, v ...interface{}) {
	if change, ok := clientStateChanges[format]; ok && len(v) == len(change.attrs) {
		attrs := make([]utils.Attribute, 0, len(v))
		for i, name := range change.attrs {
			attrs = append(attrs, utils.LogAttr(name, v[i]))
		}
		utils.LavaFormatInfo(change.message, attrs...)
		return
	}
	suppressed, due := l.due(format)
	if !due {
		return
	}
	attrs := []utils.Attribute{utils.LogAttr("detail", fmt.Sprintf(format, v...))}
	if suppressed > 0 {
		attrs = append(attrs, utils.LogAttr("suppressed", suppressed))
	}
	utils.LavaFormatWarning(clientFailureMessage, nil, attrs...)
}

// due reports whether a line of this kind is printed now, and how many of its
// kind were dropped since the last one that was.
func (l *clientLogger) due(kind string) (suppressed int, due bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	state, seen := l.kinds[kind]
	if !seen {
		if l.kinds == nil {
			l.kinds = make(map[string]*clientLogKind)
		}
		l.kinds[kind] = &clientLogKind{printedAt: now}
		return 0, true
	}
	if now.Sub(state.printedAt) < clientLogInterval {
		state.suppressed++
		return 0, false
	}
	suppressed = state.suppressed
	state.printedAt, state.suppressed = now, 0
	return suppressed, true
}
