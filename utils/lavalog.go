package utils

import (
	"context"
	"encoding/hex"
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

// DisableDebugLogBuffer turns the ring-buffer sink back off and drops the ring.
// EnableDebugLogBuffer swaps a process-global sink, so a test that enables it must
// restore this state — otherwise every later log call in the binary keeps paying the
// copy-and-append, and the sink's "disabled by default" contract is broken for
// anything that inspects it afterwards.
func DisableDebugLogBuffer() {
	disabled := zerolog.New(io.Discard).Level(zerolog.Disabled)
	debugBufferLogger.Store(&disabled)
	debugRingMu.Lock()
	debugRing = nil
	debugRingMu.Unlock()
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

func LogAttr(key string, value interface{}) Attribute {
	return Attribute{Key: key, Value: value}
}

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixNano
	setStderrLogger(time.StampNano)
}

// setStderrLogger rebuilds the global stderr logger at defaultGlobalLogLevel. timeFormat applies to
// the console format only: init uses nanoseconds, SetGlobalLoggingLevel seconds.
func setStderrLogger(timeFormat string) {
	if JsonFormat {
		zerologlog.Logger = zerologlog.Output(os.Stderr).Level(defaultGlobalLogLevel)
	} else {
		zerologlog.Logger = zerologlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: NoColor, TimeFormat: timeFormat}).Level(defaultGlobalLogLevel)
	}
}

// logLevels are the level names the --log-level flags accept.
var logLevels = map[string]zerolog.Level{
	"trace": zerolog.TraceLevel,
	"debug": zerolog.DebugLevel,
	"info":  zerolog.InfoLevel,
	"warn":  zerolog.WarnLevel,
	"error": zerolog.ErrorLevel,
	"fatal": zerolog.FatalLevel,
}

// getLogLevel reads a level name; an unknown one is info.
func getLogLevel(logLevel string) zerolog.Level {
	if level, ok := logLevels[logLevel]; ok {
		return level
	}
	return zerolog.InfoLevel
}

func SetGlobalLoggingLevel(logLevel string) {
	// setting global level prevents us from having two different levels for example one for stdout and one for rolling log.
	// zerolog.SetGlobalLevel(getLogLevel(logLevel))
	defaultGlobalLogLevel = getLogLevel(logLevel)
	setStderrLogger(time.Stamp)
	LavaFormatInfo("setting log level", Attribute{Key: "loglevel", Value: logLevel})
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
	if rollingLogLevel == "off" {
		return func() {} // default is disabled.
	}
	logLevel, ok := logLevels[rollingLogLevel]
	if !ok {
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

	// One event per sink: stderr, then the rolling log and the debug buffer when they are on.
	// stderr comes first because a PANIC or FATAL event ends the process on Msg; the debug
	// buffer records those two as errors.
	var events [3]*zerolog.Event
	events[0] = eventAt(&zerologlog.Logger, severity)
	if rollingLogLogger.GetLevel() != zerolog.Disabled {
		events[1] = eventAt(&rollingLogLogger, severity)
	}
	if dbgLogger := debugBufferLogger.Load(); dbgLogger.GetLevel() != zerolog.Disabled {
		debugSeverity := severity
		if severity == LAVA_LOG_PANIC || severity == LAVA_LOG_FATAL {
			debugSeverity = LAVA_LOG_ERROR
		}
		events[2] = eventAt(dbgLogger, debugSeverity)
	}
	// Everything below is written to a log sink or embedded in the returned
	// error's message, and upstream node-urls carry api keys in their path and
	// query. Redact once here rather than at every call site.
	description = RedactSecrets(description)
	output := description
	attrStrings := []string{}
	if err != nil {
		// The log sinks get the redacted message; the returned error keeps err
		// itself as its cause, so errors.Is/As are unaffected.
		logErr := RedactSecretsErr(err)
		for i := range events {
			events[i] = events[i].Err(logErr)
		}
		output = fmt.Sprintf("%s ErrMsg: %s", output, logErr.Error())
	}
	if len(attributes) > 0 {
		for idx, attr := range attributes {
			key := attr.Key
			val := attr.Value
			st_val := RedactSecrets(StrValueForLog(val, key, idx, attributes))
			for i := range events {
				events[i] = events[i].Str(key, st_val)
			}
			attrStrings = append(attrStrings, fmt.Sprintf("%s:%s", attr.Key, st_val))
		}
		attributesStr := "{" + strings.Join(attrStrings, ",") + "}"
		output = fmt.Sprintf("%s %+v", output, attributesStr)
	}
	for _, event := range events {
		event.Msg(description)
	}
	// Return a wrappedLavaError that supports both Unwrap() (stdlib) and
	// Cause() (pkg-errors causer interface), preserving error chain
	// traversal for callers using errors.Is().
	if err != nil {
		return &wrappedLavaError{msg: output, cause: err}
	}
	return fmt.Errorf("%s", output)
}

// eventAt starts an event on logger at a LAVA_LOG_* severity; nil for any other.
func eventAt(logger *zerolog.Logger, severity uint) *zerolog.Event {
	switch severity {
	case LAVA_LOG_PANIC:
		return logger.Panic()
	case LAVA_LOG_FATAL:
		return logger.Fatal()
	case LAVA_LOG_ERROR:
		return logger.Error()
	case LAVA_LOG_WARN:
		return logger.Warn()
	case LAVA_LOG_INFO:
		return logger.Info()
	case LAVA_LOG_DEBUG:
		return logger.Debug()
	case LAVA_LOG_TRACE:
		return logger.Trace()
	}
	return nil
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

// LavaFormatErrorSilent returns the error LavaFormatError returns, word for word, and writes no
// line: for a failure a declared event already records, which a second line would count twice.
func LavaFormatErrorSilent(description string, err error, attributes ...Attribute) error {
	output := RedactSecrets(description)
	if err != nil {
		output = fmt.Sprintf("%s ErrMsg: %s", output, RedactSecretsErr(err).Error())
	}
	if len(attributes) > 0 {
		attrStrings := make([]string, 0, len(attributes))
		for idx, attr := range attributes {
			attrStrings = append(attrStrings, fmt.Sprintf("%s:%s", attr.Key, RedactSecrets(StrValueForLog(attr.Value, attr.Key, idx, attributes))))
		}
		output = fmt.Sprintf("%s %+v", output, "{"+strings.Join(attrStrings, ",")+"}")
	}
	if err != nil {
		return &wrappedLavaError{msg: output, cause: err}
	}
	return fmt.Errorf("%s", output)
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

// LavaFormatDebugErr logs at debug severity while keeping err as the returned error's
// cause, so errors.Is/As still see it through the wrap. Passing err as an Attribute to
// LavaFormatDebug instead severs the chain: the attribute form returns a plain
// fmt.Errorf with no Unwrap, and typed errors (rate limits above all) become unreadable.
func LavaFormatDebugErr(description string, err error, attributes ...Attribute) error {
	return LavaFormatLog(description, err, attributes, LAVA_LOG_DEBUG)
}

func LavaFormatTrace(description string, attributes ...Attribute) error {
	return LavaFormatLog(description, nil, attributes, LAVA_LOG_TRACE)
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
	return hex.EncodeToString([]byte(hash))
}
