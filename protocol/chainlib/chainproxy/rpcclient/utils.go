package rpcclient

import (
	"github.com/magma-Devs/smart-router/utils/sigs"
)

func CreateHashFromParams(params []byte) string {
	return string(sigs.HashMsg(params))
}
