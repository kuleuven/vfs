package iofs

import (
	"io/fs"

	"github.com/kuleuven/vfs"
)

type FileInfo struct {
	fs.FileInfo
}

func (f *FileInfo) Uid() uint32 { //nolint:staticcheck
	return 0
}

func (f *FileInfo) Gid() uint32 { //nolint:staticcheck
	return 0
}

func (f *FileInfo) NumLinks() uint64 {
	return 1
}

func (f *FileInfo) Extended() (vfs.Attributes, error) {
	return vfs.Attributes{}, nil
}

func (f *FileInfo) Permissions() (*vfs.Permissions, error) {
	// Return bogus values, we cannot determine them
	return &vfs.Permissions{
		Read:             true,
		Write:            true,
		Delete:           true,
		Own:              true,
		GetExtendedAttrs: true,
		SetExtendedAttrs: true,
	}, nil
}
