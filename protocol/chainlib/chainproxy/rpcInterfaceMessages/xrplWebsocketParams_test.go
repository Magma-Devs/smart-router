package rpcInterfaceMessages

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlattenXRPLWebSocketParams(t *testing.T) {
	tests := []struct {
		name       string
		params     interface{}
		wantFields map[string]interface{}
		wantOk     bool
	}{
		{
			name:       "rippled's single params object flattens",
			params:     []interface{}{map[string]interface{}{"ledger_index": "validated"}},
			wantFields: map[string]interface{}{"ledger_index": "validated"},
			wantOk:     true,
		},
		{
			name:   "no params (a no-argument command) does not flatten",
			params: nil,
			wantOk: false,
		},
		{
			name:   "more than one params element does not flatten",
			params: []interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"b": 2}},
			wantOk: false,
		},
		{
			name:   "a params array of a non-object element does not flatten",
			params: []interface{}{"validated"},
			wantOk: false,
		},
		{
			name:   "a params map (not an array) does not flatten",
			params: map[string]interface{}{"ledger_index": "validated"},
			wantOk: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, ok := FlattenXRPLWebSocketParams(tt.params)
			assert.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				assert.Equal(t, tt.wantFields, fields)
			}
		})
	}
}
