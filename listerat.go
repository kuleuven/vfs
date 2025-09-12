package vfs

import (
	"io"
)

type FileInfoListerAt []FileInfo

// Modeled after strings.Reader's ReadAt() implementation
func (f FileInfoListerAt) ListAt(ls []FileInfo, offset int64) (int, error) {
	var n int

	if offset >= int64(len(f)) {
		return 0, io.EOF
	}

	n = copy(ls, f[offset:])

	if n < len(ls) {
		return n, io.EOF
	}

	return n, nil
}

func (f FileInfoListerAt) Close() error {
	return nil
}
