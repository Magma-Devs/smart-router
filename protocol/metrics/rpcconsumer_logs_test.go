package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	websocket2 "github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type WebSocketError struct {
	ErrorReceived string `json:"Error_Received"`
}

type ErrorData struct {
	GUID   string `json:"Error_GUID"`
	Error1 string `json:"Error"`
}

func TestGetUniqueGuidResponseForError(t *testing.T) {
	plog, err := NewRPCConsumerLogs(nil, nil, nil, nil)
	assert.Nil(t, err)

	responseError := errors.New("response error")

	errorMsg := plog.GetUniqueGuidResponseForError(responseError, "msgSeed")

	errObject := &ErrorData{}

	err = json.Unmarshal([]byte(errorMsg), errObject)
	assert.Nil(t, err)
	assert.Equal(t, errObject.GUID, "msgSeed")
	assert.Equal(t, errObject.Error1, "response error")
}

func TestGetUniqueGuidResponseDeterministic(t *testing.T) {
	plog, err := NewRPCConsumerLogs(nil, nil, nil, nil)
	assert.Nil(t, err)

	responseError := errors.New("response error")
	utils.SetGlobalLoggingLevel("fatal") // prevent spam.
	errorMsg := plog.GetUniqueGuidResponseForError(responseError, "msgSeed")

	for i := 1; i < 10000; i++ {
		err := plog.GetUniqueGuidResponseForError(responseError, "msgSeed")

		assert.Equal(t, err, errorMsg)
	}
}

func TestAnalyzeWebSocketErrorAndWriteMessage(t *testing.T) {
	app := fiber.New()

	app.Get("/", websocket.New(func(c *websocket.Conn) {
		mt, _, _ := c.ReadMessage()
		plog, _ := NewRPCConsumerLogs(nil, nil, nil, nil)
		responseError := errors.New("response error")
		formatterMsg := plog.AnalyzeWebSocketErrorAndGetFormattedMessage(c.LocalAddr().String(), responseError, "seed", []byte{}, "rpcType", 1*time.Millisecond)
		assert.NotNil(t, formatterMsg)
		c.WriteMessage(mt, formatterMsg)
	}))

	listenFunc := func() {
		address := "127.0.0.1:3000"
		err := app.Listen(address)
		if err != nil {
			utils.LavaFormatError("can't listen in unitests", err, utils.Attribute{Key: "address", Value: address})
		}
	}
	go listenFunc()
	defer func() {
		app.Shutdown()
	}()
	time.Sleep(time.Millisecond * 100)
	url := "ws://127.0.0.1:3000/"
	dialer := &websocket2.Dialer{}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Error dialing websocket connection: %s", err)
	}
	defer conn.Close()

	err = conn.WriteMessage(websocket.TextMessage, []byte("test"))
	if err != nil {
		t.Fatalf("Error writing message to websocket connection: %s", err)
	}

	_, response, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("Error reading message from websocket connection: %s", err)
	}

	errObject := &WebSocketError{}

	err = json.Unmarshal(response, errObject)
	assert.Nil(t, err)

	errData := &ErrorData{}
	err = json.Unmarshal([]byte(errObject.ErrorReceived), errData)
	assert.Nil(t, err)
	assert.Equal(t, errData.GUID, "seed")
}

// captureStderr swaps os.Stderr with a pipe while fn runs, returning what was
// written. Required because utils.LavaFormat* logs via the global zerolog
// logger bound to os.Stderr at SetGlobalLoggingLevel() time, and is not safe
// for parallel tests (os.Stderr is process-global).
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		io.Copy(&buf, r)
		close(done)
	}()

	fn()

	w.Close()
	<-done
	r.Close()
	os.Stderr = origStderr
	return buf.String()
}

// TestSetGlobalLoggingLevel_VolumeChangesAcrossLevels backfills MAG-1872 item
// 7: --log-level info/debug/warn parametrization. Existing tests in this file
// only exercise "fatal". This asserts that LavaFormatLog's early-return
// optimization filters lower-severity messages at higher levels, and emits
// them at lower levels.
func TestSetGlobalLoggingLevel_VolumeChangesAcrossLevels(t *testing.T) {
	origFormat := utils.JsonFormat
	utils.JsonFormat = true // JSON output is easier to grep deterministically
	t.Cleanup(func() {
		utils.JsonFormat = origFormat
		utils.SetGlobalLoggingLevel("info")
	})

	const (
		debugMarker = "level-test-debug-marker"
		infoMarker  = "level-test-info-marker"
		warnMarker  = "level-test-warning-marker"
	)

	cases := []struct {
		level     string
		wantDebug bool
		wantInfo  bool
		wantWarn  bool
	}{
		{level: "debug", wantDebug: true, wantInfo: true, wantWarn: true},
		{level: "info", wantDebug: false, wantInfo: true, wantWarn: true},
		{level: "warn", wantDebug: false, wantInfo: false, wantWarn: true},
	}

	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			out := captureStderr(t, func() {
				utils.SetGlobalLoggingLevel(tc.level)
				utils.LavaFormatDebug(debugMarker)
				utils.LavaFormatInfo(infoMarker)
				utils.LavaFormatWarning(warnMarker, nil)
			})
			assert.Equal(t, tc.wantDebug, strings.Contains(out, debugMarker),
				"debug visibility wrong at level=%s, output=%s", tc.level, out)
			assert.Equal(t, tc.wantInfo, strings.Contains(out, infoMarker),
				"info visibility wrong at level=%s, output=%s", tc.level, out)
			assert.Equal(t, tc.wantWarn, strings.Contains(out, warnMarker),
				"warn visibility wrong at level=%s, output=%s", tc.level, out)
		})
	}
}

// TestSetGlobalLoggingLevel_TextVsJsonFormat backfills MAG-1872 item 8:
// --log-format text|json flag. The flag is wired at
// protocol/rpcsmartrouter/rpcsmartrouter.go and consumed via utils.JsonFormat,
// which gates the writer choice in SetGlobalLoggingLevel. This asserts that
// the produced output is JSON-parseable iff JsonFormat=true.
func TestSetGlobalLoggingLevel_TextVsJsonFormat(t *testing.T) {
	origFormat := utils.JsonFormat
	t.Cleanup(func() {
		utils.JsonFormat = origFormat
		utils.SetGlobalLoggingLevel("info")
	})

	const marker = "format-test-marker"

	cases := []struct {
		name string
		json bool
	}{
		{name: "json_format", json: true},
		{name: "text_format", json: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			utils.JsonFormat = tc.json
			out := captureStderr(t, func() {
				utils.SetGlobalLoggingLevel("info")
				utils.LavaFormatInfo(marker)
			})

			var markerLine string
			for _, line := range strings.Split(out, "\n") {
				if strings.Contains(line, marker) {
					markerLine = line
					break
				}
			}
			require.NotEmpty(t, markerLine, "marker line not captured, output=%s", out)

			var parsed map[string]interface{}
			err := json.Unmarshal([]byte(markerLine), &parsed)
			if tc.json {
				require.NoError(t, err, "json format must produce parseable JSON, line=%s", markerLine)
				assert.Equal(t, "info", parsed["level"], "json format must carry structured level field")
				assert.Equal(t, marker, parsed["message"], "json format must carry structured message field")
			} else {
				require.Error(t, err, "text format must NOT parse as JSON, line=%s", markerLine)
				assert.Contains(t, markerLine, "INF",
					"console format must contain abbreviated level marker, line=%s", markerLine)
			}
		})
	}
}

// TestLavaFormat_GUIDAttachedToNormalLogPaths backfills MAG-1872 item 9: GUID
// presence in non-error log lines. Existing tests assert Error_GUID in error
// JSON only; this asserts that utils.LogAttr("GUID", ctx) on an Info log
// surfaces the GUID as a top-level field via utils.StrValueForLog's GUID
// extraction path (utils/lavalog.go:182).
func TestLavaFormat_GUIDAttachedToNormalLogPaths(t *testing.T) {
	origFormat := utils.JsonFormat
	utils.JsonFormat = true
	t.Cleanup(func() {
		utils.JsonFormat = origFormat
		utils.SetGlobalLoggingLevel("info")
	})

	const (
		testGUID uint64 = 0xDEADBEEFCAFE
		marker          = "guid-on-normal-path-marker"
	)
	ctx := utils.WithUniqueIdentifier(context.Background(), testGUID)

	out := captureStderr(t, func() {
		utils.SetGlobalLoggingLevel("info")
		utils.LavaFormatInfo(marker, utils.LogAttr("GUID", ctx))
	})

	var markerLine string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, marker) {
			markerLine = line
			break
		}
	}
	require.NotEmpty(t, markerLine, "marker line not captured, output=%s", out)

	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(markerLine), &parsed),
		"info log must be JSON parseable, line=%s", markerLine)

	// StrValueForLog stringifies the GUID via strconv.FormatUint(guid, 10).
	guidStr, ok := parsed["GUID"].(string)
	require.True(t, ok, "GUID field must be a string in JSON output, got %T (line=%s)", parsed["GUID"], markerLine)
	assert.Equal(t, "244837814094590", guidStr,
		"GUID field must equal decimal-stringified context GUID")

	// Contrast with the error-path tests above which look for Error_GUID; the
	// normal-path key is the literal "GUID".
	_, hasErrorGUID := parsed["Error_GUID"]
	assert.False(t, hasErrorGUID, "normal-path log must not carry Error_GUID field")
}
