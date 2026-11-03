package utils

import (
	zerolog "github.com/rs/zerolog"
)

// LogEvent writes a declared event, a line a reader parses by its fields rather than its text, at
// INFO on every enabled sink whatever the configured log level: a level filter would make a contract
// optional. It redacts nothing, so fields must carry only redacted text.
func LogEvent(message string, fields zerolog.LogObjectMarshaler) {
	std := stdLogger.Load().Level(zerolog.TraceLevel)
	std.Info().EmbedObject(fields).Msg(message)
	if rollingLogLogger.GetLevel() != zerolog.Disabled {
		rolling := rollingLogLogger.Level(zerolog.TraceLevel)
		rolling.Info().EmbedObject(fields).Msg(message)
	}
	if dbgLogger := debugBufferLogger.Load(); dbgLogger.GetLevel() != zerolog.Disabled {
		dbgLogger.Info().EmbedObject(fields).Msg(message)
	}
}
