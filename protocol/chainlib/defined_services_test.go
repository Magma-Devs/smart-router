package chainlib

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// DefinedServices keeps the url's own order and names each unknown name once.
func TestDefinedServices(t *testing.T) {
	ctrl := gomock.NewController(t)
	parser := NewMockChainParser(ctrl)
	parser.EXPECT().SeparateAddonsExtensions(gomock.Any(), []string{"debug", "archiv", "archive", "archiv"}).
		Return([]string{"debug"}, []string{"archive"}, errors.New("archiv is neither an addon nor an extension"))
	parser.EXPECT().SeparateAddonsExtensions(gomock.Any(), []string{"debug"}).Return([]string{"debug"}, nil, nil)

	defined, unknown := DefinedServices(parser, []string{"debug", "archiv", "archive", "archiv"})
	require.Equal(t, []string{"debug", "archive"}, defined)
	require.Equal(t, []string{"archiv"}, unknown)

	declared := []string{"debug"}
	defined, unknown = DefinedServices(parser, declared)
	require.Equal(t, declared, defined)
	require.Nil(t, unknown)

	defined, unknown = DefinedServices(parser, nil)
	require.Nil(t, defined)
	require.Nil(t, unknown)
}
