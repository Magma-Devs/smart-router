package rpcInterfaceMessages

import (
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReflectionSupport(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name   string
		err    error
		result error
	}{
		{
			name:   "nil error",
			err:    nil,
			result: nil,
		},
		{
			name:   "non-unimplemented error",
			err:    fmt.Errorf("error"),
			result: fmt.Errorf("error"),
		},
		{
			name: "unimplemented error",
			err:  status.Error(codes.Unimplemented, "unimplemented"),
			result: utils.LavaFormatError("server does not support the reflection API",
				status.Error(codes.Unimplemented, "unimplemented")),
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := ReflectionSupport(testCase.err)

			if testCase.err == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, testCase.result.Error(), result.Error())
			}
		})
	}
}

func TestParseSymbol(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name      string
		input     string
		expectedS string
		expectedM string
	}{
		{
			name:      "Parse ServiceName/MethodName",
			input:     "ServiceName/MethodName",
			expectedS: "ServiceName",
			expectedM: "MethodName",
		},
		{
			name:      "Parse ServiceName.MethodName",
			input:     "ServiceName.MethodName",
			expectedS: "ServiceName",
			expectedM: "MethodName",
		},
		{
			name:      "Parse ServiceName",
			input:     "ServiceName",
			expectedS: "",
			expectedM: "",
		},
		{
			name:      "Parse empty string",
			input:     "",
			expectedS: "",
			expectedM: "",
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			s, m := ParseSymbol(testCase.input)
			if s != testCase.expectedS {
				t.Errorf("expected %q, but got %q", testCase.expectedS, s)
			}
			if m != testCase.expectedM {
				t.Errorf("expected %q, but got %q", testCase.expectedM, m)
			}
		})
	}
}
