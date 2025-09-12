package vfs

import (
	"fmt"
	"syscall"
)

var ErrNotSupported = syscall.EOPNOTSUPP

var ErrNotImplemented = fmt.Errorf("%w: not implemented", syscall.EPERM)
