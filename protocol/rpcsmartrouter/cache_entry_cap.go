package rpcsmartrouter

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/spf13/viper"
)

// cacheMaxEntryBytesFrom reads --cache-max-entry-bytes from the flag or the config file. A
// value that is not a whole, non-negative byte count is refused rather than read as 0, which
// would silently lift the cap.
func cacheMaxEntryBytesFrom(v *viper.Viper) (int64, error) {
	raw := strings.TrimSpace(v.GetString(common.CacheMaxEntryBytesFlagName))
	maxEntryBytes, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || maxEntryBytes < 0 {
		return 0, fmt.Errorf("%s must be a whole number of bytes, 0 for no cap: got %q", common.CacheMaxEntryBytesFlagName, raw)
	}
	return maxEntryBytes, nil
}
