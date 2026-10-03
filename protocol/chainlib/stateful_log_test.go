package chainlib

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// The value the JSON log line carries: the log collector labels a line by the
// exact text "stateful":"1", the same text "Choosing providers" writes.
func TestStatefulLogAttr(t *testing.T) {
	logged := func(pm ProtocolMessage) string {
		attr := StatefulLogAttr(pm)
		require.Equal(t, "stateful", attr.Key)
		return utils.StrValueForLog(attr.Value, attr.Key, 0, []utils.Attribute{attr})
	}
	write := NewProtocolMessage(&baseChainMessageContainer{api: &spectypes.Api{Category: spectypes.SpecCategory{Stateful: 1}}}, nil, nil, "", "")
	read := NewProtocolMessage(&baseChainMessageContainer{api: &spectypes.Api{}}, nil, nil, "", "")
	noApi := NewProtocolMessage(&baseChainMessageContainer{}, nil, nil, "", "")

	require.Equal(t, "1", logged(write))
	require.Equal(t, "0", logged(read))
	require.Equal(t, "0", logged(noApi))
	require.Equal(t, "0", logged(nil))
}
