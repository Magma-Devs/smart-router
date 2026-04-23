package rpcclient

import (
	"github.com/Magma-Devs/smart-router/utils/sigs"
)

func CreateHashFromParams(params []byte) string {
	return string(sigs.HashMsg(params))
}
