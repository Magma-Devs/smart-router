package protocolerrors

import "errors"

var ConsistencyError = errors.New("does not meet consistency requirements")
