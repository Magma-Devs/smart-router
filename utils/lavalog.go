package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	zerolog "github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	EventPrefix = "lava_"
)

// wrappedLavaError wraps a cause error and preserves error chain traversal
// for callers using errors.Is() or pkg-errors Cause().
type wrappedLavaError struct {
	msg   string
	cause error
}

func (e *wrappedLavaError) Error() string { return e.msg }
func (e *wrappedLavaError) Cause() error  { return e.cause }
func (e *wrappedLavaError) Unwrap() error { return e.cause }

const (
	LAVA_LOG_TRACE = iota
	LAVA_LOG_DEBUG
	LAVA_LOG_INFO
	LAVA_LOG_WARN
	LAVA_LOG_ERROR
	LAVA_LOG_FATAL
	LAVA_LOG_PANIC
	LAVA_LOG_PRODUCTION
	NoColor = true
)

const (
	KEY_REQUEST_ID     = "request_id"
	KEY_TASK_ID        = "task_id"
	KEY_TRANSACTION_ID = "tx_id"
)

var (
	JsonFormat = false
	// if set to production, this will replace some errors to warning that can be caused by misuse instead of bugs
	ExtendedLogLevel      = "development"
	rollingLogLogger      = zerolog.New(os.Stderr).Level(zerolog.Disabled) // this is the singleton rolling logger.
	defaultGlobalLogLevel = zerolog.DebugLevel
)

// debugRingWriter is a mutex-guarded fixed-capacity circular buffer of raw log
// records. It implements io.Writer so a zerolog sink can write JSON records into
// it. Used only in debug mode (--debug-address) as a forensic in-memory log
// buffer fetched via /debug/logs.
type debugRingWriter struct {
	mu  sync.Mutex
	buf [][]byte
	cap int
}

func newDebugRingWriter(capacity int) *debugRingWriter {
	if capacity <= 0 {
		capacity = 1
	}
	return &debugRingWriter{
		buf: make([][]byte, 0, capacity),
		cap: capacity,
	}
}

// Write copies p (zerolog reuses its write buffer, so we MUST copy) and appends
// it to the ring, evicting the oldest record when at capacity.
func (d *debugRingWriter) Write(p []byte) (int, error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	d.mu.Lock()
	if len(d.buf) >= d.cap {
		// evict the oldest record
		d.buf = d.buf[1:]
	}
	d.buf = append(d.buf, cp)
	d.mu.Unlock()
	return len(p), nil
}

// snapshot returns a copy of the current ring contents in oldest-to-newest order.
func (d *debugRingWriter) snapshot() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][]byte, len(d.buf))
	copy(out, d.buf)
	return out
}

// clear empties the ring.
func (d *debugRingWriter) clear() {
	d.mu.Lock()
	d.buf = d.buf[:0]
	d.mu.Unlock()
}

var (
	// debugBufferLogger is the THIRD zerolog sink (alongside the std logger and
	// rollingLogLogger). Disabled by default; enabled only in debug mode.
	//
	// Held in an atomic.Pointer because EnableDebugLogBuffer swaps the logger
	// at startup while goroutines spawned by Start (epoch timer, monitoring)
	// may already be logging — a plain assignment would be a data race on the
	// read sites in LavaFormatLog.
	debugBufferLogger atomic.Pointer[zerolog.Logger]
	debugRingMu       sync.Mutex
	debugRing         *debugRingWriter
)

func init() {
	disabled := zerolog.New(io.Discard).Level(zerolog.Disabled)
	debugBufferLogger.Store(&disabled)
}

// EnableDebugLogBuffer turns on the in-memory ring-buffer log sink with the
// given capacity (number of records). Debug-mode only — called from
// rpcsmartrouter.Start when --debug-address is set. Captures EVERYTHING
// (TraceLevel) as machine-parseable JSON; forensics is the point.
func EnableDebugLogBuffer(maxLines int) {
	if maxLines <= 0 {
		maxLines = 5000
	}
	debugRingMu.Lock()
	debugRing = newDebugRingWriter(maxLines)
	ring := debugRing
	debugRingMu.Unlock()
	logger := zerolog.New(ring).Level(zerolog.TraceLevel).With().Timestamp().Logger()
	debugBufferLogger.Store(&logger)
}

// ClearDebugLogBuffer drops every record currently in the ring. No-op when the
// buffer was never enabled.
func ClearDebugLogBuffer() {
	debugRingMu.Lock()
	ring := debugRing
	debugRingMu.Unlock()
	if ring != nil {
		ring.clear()
	}
}

// ReadDebugLogBuffer returns the buffered log records, filtered by requestID
// and/or time window, with the most recent `limit` records retained (tail).
//
//   - requestID != "": keep only records whose "request_id" field equals it. A
//     record that fails to JSON-parse is skipped when a requestID filter is
//     active, and kept when it is not.
//   - from/to (non-zero): keep records whose "time" field (Unix-nano integer,
//     since the package sets zerolog.TimeFieldFormat = TimeFormatUnixNano) falls
//     within [from, to].
//   - both given → ANDed.
//   - limit <= 0 defaults to 5000.
func ReadDebugLogBuffer(requestID string, from, to time.Time, limit int) [][]byte {
	if limit <= 0 {
		limit = 5000
	}
	debugRingMu.Lock()
	ring := debugRing
	debugRingMu.Unlock()
	if ring == nil {
		return [][]byte{}
	}
	records := ring.snapshot()

	filterRequest := requestID != ""
	filterFrom := !from.IsZero()
	filterTo := !to.IsZero()
	var fromNano, toNano int64
	if filterFrom {
		fromNano = from.UnixNano()
	}
	if filterTo {
		toNano = to.UnixNano()
	}

	out := make([][]byte, 0, len(records))
	for _, rec := range records {
		var fields map[string]json.RawMessage
		parsed := json.Unmarshal(rec, &fields) == nil

		if !parsed {
			// Unparseable lines: keep only when no requestID filter is active.
			if filterRequest {
				continue
			}
			out = append(out, rec)
			continue
		}

		if filterRequest {
			raw, ok := fields[KEY_REQUEST_ID]
			if !ok {
				continue
			}
			var rid string
			if err := json.Unmarshal(raw, &rid); err != nil || rid != requestID {
				continue
			}
		}

		if filterFrom || filterTo {
			raw, ok := fields["time"]
			if !ok {
				continue
			}
			var ts int64
			if err := json.Unmarshal(raw, &ts); err != nil {
				continue
			}
			if filterFrom && ts < fromNano {
				continue
			}
			if filterTo && ts > toNano {
				continue
			}
		}

		out = append(out, rec)
	}

	// Apply limit LAST: keep the most recent `limit` records (tail).
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

type Attribute struct {
	Key   string
	Value interface{}
}

func StringMapToAttributes(details map[string]string) []Attribute {
	var attrs []Attribute
	for key, val := range details {
		attrs = append(attrs, Attribute{Key: key, Value: val})
	}
	return attrs
}

func LogAttr(key string, value interface{}) Attribute {
	return Attribute{Key: key, Value: value}
}

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixNano
	if JsonFormat {
		zerologlog.Logger = zerologlog.Output(os.Stderr).Level(defaultGlobalLogLevel)
	} else {
		zerologlog.Logger = zerologlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: NoColor, TimeFormat: time.StampNano}).Level(defaultGlobalLogLevel)
	}
}

func getLogLevel(logLevel string) zerolog.Level {
	switch logLevel {
	case "trace":
		return zerolog.TraceLevel
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}

func SetGlobalLoggingLevel(logLevel string) {
	// setting global level prevents us from having two different levels for example one for stdout and one for rolling log.
	// zerolog.SetGlobalLevel(getLogLevel(logLevel))
	defaultGlobalLogLevel = getLogLevel(logLevel)
	if JsonFormat {
		zerologlog.Logger = zerologlog.Output(os.Stderr).Level(defaultGlobalLogLevel)
	} else {
		zerologlog.Logger = zerologlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: NoColor, TimeFormat: time.Stamp}).Level(defaultGlobalLogLevel)
	}
	LavaFormatInfo("setting log level", Attribute{Key: "loglevel", Value: logLevel})
}

func SetLogLevelFieldName(fieldName string) {
	zerolog.LevelFieldName = fieldName
}

// IsDebugEnabled reports whether debug-level logging is currently active.
// Use this to guard expensive pre-call computations that are only needed when
// a debug log will actually be emitted.
func IsDebugEnabled() bool {
	return defaultGlobalLogLevel <= zerolog.DebugLevel
}

func RollingLoggerSetup(rollingLogLevel string, filePath string, maxSize string, maxBackups string, maxAge string, stdFormat string) func() {
	maxSizeNumber, err := strconv.Atoi(maxSize)
	if err != nil {
		LavaFormatFatal("strconv.Atoi(maxSize)", err, LogAttr("maxSize", maxSize))
	}
	maxBackupsNumber, err := strconv.Atoi(maxBackups)
	if err != nil {
		LavaFormatFatal("strconv.Atoi(maxSize)", err, LogAttr("maxBackups", maxBackups))
	}
	maxAgeNumber, err := strconv.Atoi(maxAge)
	if err != nil {
		LavaFormatFatal("strconv.Atoi(maxSize)", err, LogAttr("maxAge", maxAge))
	}

	rollingLogOutput := &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    maxSizeNumber,
		MaxBackups: maxBackupsNumber,
		MaxAge:     maxAgeNumber,
		Compress:   true,
	}
	var logLevel zerolog.Level
	switch rollingLogLevel {
	case "off":
		return func() {} // default is disabled.
	case "trace":
		logLevel = zerolog.TraceLevel
	case "debug":
		logLevel = zerolog.DebugLevel
	case "info":
		logLevel = zerolog.InfoLevel
	case "warn":
		logLevel = zerolog.WarnLevel
	case "error":
		logLevel = zerolog.ErrorLevel
	case "fatal":
		logLevel = zerolog.FatalLevel
	default:
		LavaFormatFatal("unsupported case for rollingLoggerSetup", nil, LogAttr("rollingLogLevel", rollingLogLevel))
	}
	// set the rolling log level.
	if stdFormat == "json" {
		rollingLogLogger = zerolog.New(rollingLogOutput).Level(logLevel).With().Timestamp().Logger()
	} else {
		rollingLogLogger = zerolog.New(zerolog.ConsoleWriter{Out: rollingLogOutput, NoColor: NoColor, TimeFormat: time.Stamp}).Level(logLevel).With().Timestamp().Logger()
	}
	rollingLogLogger.Debug().Msg("Starting Rolling Logger")
	return func() { rollingLogOutput.Close() }
}

func StrValueForLog(val interface{}, key string, idx int, attributes []Attribute) string {
	st_val := ""
	switch value := val.(type) {
	case context.Context:
		// we don't want to print the whole context so change it
		switch key {
		case "GUID":
			guid, found := GetUniqueIdentifier(value)
			if found {
				st_val = strconv.FormatUint(guid, 10)
				attributes[idx] = Attribute{Key: key, Value: guid}
			} else {
				attributes[idx] = Attribute{Key: key, Value: "no-guid"}
			}
		case KEY_REQUEST_ID:
			reqId, found := GetRequestId(value)
			if found {
				st_val = reqId
				attributes[idx] = Attribute{Key: key, Value: reqId}
			} else {
				attributes[idx] = Attribute{Key: key, Value: ""}
			}
		case KEY_TASK_ID:
			taskId, found := GetTaskId(value)
			if found {
				st_val = taskId
				attributes[idx] = Attribute{Key: key, Value: taskId}
			} else {
				attributes[idx] = Attribute{Key: key, Value: ""}
			}
		case KEY_TRANSACTION_ID:
			txId, found := GetTxId(value)
			if found {
				st_val = txId
				attributes[idx] = Attribute{Key: key, Value: txId}
			} else {
				attributes[idx] = Attribute{Key: key, Value: ""}
			}
		default:
			attributes[idx] = Attribute{Key: key, Value: "context-masked"}
		}
	default:
		st_val = StrValue(val)
	}
	return st_val
}

func StrValue(val interface{}) string {
	st_val := ""
	switch value := val.(type) {
	case context.Context:
		// we don't want to print the whole context so change it
	case bool:
		if value {
			st_val = "true"
		} else {
			st_val = "false"
		}
	case fmt.Stringer:
		// Use a defer/recover to catch panics from String() method
		// This is particularly important for protobuf messages which can panic
		// if they have nil internal fields
		func() {
			defer func() {
				if r := recover(); r != nil {
					LavaFormatWarning("protobuf String() panic detected",
						fmt.Errorf("panic: %v", r),
						LogAttr("type", fmt.Sprintf("%T", value)))
					st_val = fmt.Sprintf("<panic in String(): %v>", r)
				}
			}()
			st_val = value.String()
		}()
	case string:
		st_val = value
	case int:
		st_val = strconv.Itoa(value)
	case int64:
		st_val = strconv.FormatInt(value, 10)
	case uint64:
		st_val = strconv.FormatUint(value, 10)
	case error:
		st_val = value.Error()
	case []error:
		for _, err := range value {
			if err == nil {
				continue
			}
			st_val += err.Error() + ";"
		}
	case []string:
		st_val = strings.Join(value, ",")
	// needs to come after stringer so byte inheriting objects will use their string method if implemented (like AccAddress)
	case []byte:
		st_val = string(value)
	case nil:
		st_val = ""
	default:
		st_val = fmt.Sprintf("%+v", value)
	}
	return st_val
}

func LavaFormatLog(description string, err error, attributes []Attribute, severity uint) error {
	// OPTIMIZATION: Early return if log level is not enabled
	// This prevents expensive string formatting and attribute processing
	// when logs would be filtered anyway
	// depending on the build flag, this log function will log either a warning or an error.
	// the purpose of this function is to fail E2E tests and not allow unexpected behavior to reach main.
	// while in production some errors may occur as consumers / providers might set up their processes in the wrong way.
	// in test environment we don't expect to have these errors and if they occur we would like to fail the test.
	if severity == LAVA_LOG_PRODUCTION {
		if ExtendedLogLevel == "production" {
			severity = LAVA_LOG_WARN
		} else {
			severity = LAVA_LOG_ERROR
		}
	}

	// Check if this log level is enabled before doing expensive work
	var logLevelEnabled bool
	switch severity {
	case LAVA_LOG_TRACE:
		logLevelEnabled = defaultGlobalLogLevel <= zerolog.TraceLevel
	case LAVA_LOG_DEBUG:
		logLevelEnabled = defaultGlobalLogLevel <= zerolog.DebugLevel
	case LAVA_LOG_INFO:
		logLevelEnabled = defaultGlobalLogLevel <= zerolog.InfoLevel
	case LAVA_LOG_WARN:
		logLevelEnabled = defaultGlobalLogLevel <= zerolog.WarnLevel
	case LAVA_LOG_ERROR, LAVA_LOG_FATAL, LAVA_LOG_PANIC:
		logLevelEnabled = true // Always process errors, fatal, and panic
	default:
		logLevelEnabled = true
	}

	// Early return if log level not enabled - skip all expensive formatting
	if !logLevelEnabled {
		if err != nil {
			return err // Return original error without formatting
		}
		// Still return an error based on description to maintain the contract
		// that LavaFormat* functions always return an error when called
		return fmt.Errorf("%s", description)
	}
	// if JsonFormat {
	// 	zerologlog.Logger = zerologlog.Output(os.Stderr).Level(defaultGlobalLogLevel)
	// } else {
	// 	zerologlog.Logger = zerologlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: NoColor, TimeFormat: time.Stamp}).Level(defaultGlobalLogLevel)
	// }

	var logEvent *zerolog.Event
	var rollingLoggerEvent *zerolog.Event
	var debugBufferEvent *zerolog.Event
	rollingLogEnabled := rollingLogLogger.GetLevel() != zerolog.Disabled
	dbgLogger := debugBufferLogger.Load()
	debugBufferEnabled := dbgLogger.GetLevel() != zerolog.Disabled
	switch severity {
	case LAVA_LOG_PANIC:
		logEvent = zerologlog.Panic()
		if rollingLogEnabled {
			rollingLoggerEvent = rollingLogLogger.Panic()
		}
		if debugBufferEnabled {
			debugBufferEvent = dbgLogger.Error()
		}
	case LAVA_LOG_FATAL:
		logEvent = zerologlog.Fatal()
		if rollingLogEnabled {
			rollingLoggerEvent = rollingLogLogger.Fatal()
		}
		if debugBufferEnabled {
			debugBufferEvent = dbgLogger.Error()
		}
	case LAVA_LOG_ERROR:
		logEvent = zerologlog.Error()
		if rollingLogEnabled {
			rollingLoggerEvent = rollingLogLogger.Error()
		}
		if debugBufferEnabled {
			debugBufferEvent = dbgLogger.Error()
		}
	case LAVA_LOG_WARN:
		logEvent = zerologlog.Warn()
		if rollingLogEnabled {
			rollingLoggerEvent = rollingLogLogger.Warn()
		}
		if debugBufferEnabled {
			debugBufferEvent = dbgLogger.Warn()
		}
	case LAVA_LOG_INFO:
		logEvent = zerologlog.Info()
		if rollingLogEnabled {
			rollingLoggerEvent = rollingLogLogger.Info()
		}
		if debugBufferEnabled {
			debugBufferEvent = dbgLogger.Info()
		}
	case LAVA_LOG_DEBUG:
		logEvent = zerologlog.Debug()
		if rollingLogEnabled {
			rollingLoggerEvent = rollingLogLogger.Debug()
		}
		if debugBufferEnabled {
			debugBufferEvent = dbgLogger.Debug()
		}
	case LAVA_LOG_TRACE:
		logEvent = zerologlog.Trace()
		if rollingLogEnabled {
			rollingLoggerEvent = rollingLogLogger.Trace()
		}
		if debugBufferEnabled {
			debugBufferEvent = dbgLogger.Trace()
		}
	}
	output := description
	attrStrings := []string{}
	if err != nil {
		logEvent = logEvent.Err(err)
		if rollingLoggerEvent != nil {
			rollingLoggerEvent = rollingLoggerEvent.Err(err)
		}
		if debugBufferEvent != nil {
			debugBufferEvent = debugBufferEvent.Err(err)
		}
		output = fmt.Sprintf("%s ErrMsg: %s", output, err.Error())
	}
	if len(attributes) > 0 {
		for idx, attr := range attributes {
			key := attr.Key
			val := attr.Value
			st_val := StrValueForLog(val, key, idx, attributes)
			logEvent = logEvent.Str(key, st_val)
			if rollingLoggerEvent != nil {
				rollingLoggerEvent = rollingLoggerEvent.Str(key, st_val)
			}
			if debugBufferEvent != nil {
				debugBufferEvent = debugBufferEvent.Str(key, st_val)
			}
			attrStrings = append(attrStrings, fmt.Sprintf("%s:%s", attr.Key, st_val))
		}
		attributesStr := "{" + strings.Join(attrStrings, ",") + "}"
		output = fmt.Sprintf("%s %+v", output, attributesStr)
	}
	logEvent.Msg(description)
	if rollingLoggerEvent != nil {
		rollingLoggerEvent.Msg(description)
	}
	if debugBufferEvent != nil {
		debugBufferEvent.Msg(description)
	}
	// Return a wrappedLavaError that supports both Unwrap() (stdlib) and
	// Cause() (pkg-errors causer interface), preserving error chain
	// traversal for callers using errors.Is().
	if err != nil {
		return &wrappedLavaError{msg: output, cause: err}
	}
	return fmt.Errorf("%s", output)
}

func LavaFormatPanic(description string, err error, attributes ...Attribute) {
	attributes = append(attributes, Attribute{Key: "StackTrace", Value: debug.Stack()})
	LavaFormatLog(description, err, attributes, LAVA_LOG_PANIC)
}

func LavaFormatFatal(description string, err error, attributes ...Attribute) {
	attributes = append(attributes, Attribute{Key: "StackTrace", Value: debug.Stack()})
	LavaFormatLog(description, err, attributes, LAVA_LOG_FATAL)
}

// see documentation in LavaFormatLog function
func LavaFormatProduction(description string, err error, attributes ...Attribute) error {
	return LavaFormatLog(description, err, attributes, LAVA_LOG_PRODUCTION)
}

func LavaFormatError(description string, err error, attributes ...Attribute) error {
	return LavaFormatLog(description, err, attributes, LAVA_LOG_ERROR)
}

func LavaFormatWarning(description string, err error, attributes ...Attribute) error {
	return LavaFormatLog(description, err, attributes, LAVA_LOG_WARN)
}

func LavaFormatInfo(description string, attributes ...Attribute) error {
	return LavaFormatLog(description, nil, attributes, LAVA_LOG_INFO)
}

func LavaFormatDebug(description string, attributes ...Attribute) error {
	return LavaFormatLog(description, nil, attributes, LAVA_LOG_DEBUG)
}

func LavaFormatTrace(description string, attributes ...Attribute) error {
	return LavaFormatLog(description, nil, attributes, LAVA_LOG_TRACE)
}

func IsTraceLogLevelEnabled() bool {
	return defaultGlobalLogLevel == zerolog.TraceLevel
}

func FormatStringerList[T fmt.Stringer](description string, listToPrint []T, separator string) string {
	st := ""
	for _, printable := range listToPrint {
		st = st + separator + printable.String() + "\n"
	}
	st = fmt.Sprintf(description+"\n\t%s", st)
	return st
}

func FormatLongString(msg string, maxCharacters int) string {
	if maxCharacters != 0 && len(msg) > maxCharacters {
		postfixLen := maxCharacters / 3
		prefixLen := maxCharacters - postfixLen
		return msg[:prefixLen] + "...truncated..." + msg[len(msg)-postfixLen:]
	}
	return msg
}

func ToHexString(hash string) string {
	return fmt.Sprintf("%x", hash)
}
