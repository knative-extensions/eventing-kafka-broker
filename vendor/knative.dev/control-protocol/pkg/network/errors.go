package network

import (
	"errors"
	"io"
	"net"
)

func isTransientError(err error) bool {
	// Transient errors are fine
	if neterr, ok := err.(net.Error); ok {
		if neterr.Temporary() || neterr.Timeout() {
			return true
		}
	}
	return false
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
