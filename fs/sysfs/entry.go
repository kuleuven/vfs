package sysfs

import (
	"io"
	"os"
	"syscall"
	"time"

	"github.com/kuleuven/vfs"
)

type Entry struct {
	Name    string
	Payload []byte
}

func (e Entry) Info() vfs.FileInfo {
	return entryInfo{
		name: e.Name,
		size: int64(len(e.Payload)),
	}
}

func (e Entry) File() vfs.File {
	return &entryFile{
		entry:  e,
		offset: 0,
	}
}

type entryFile struct {
	entry  Entry
	offset int64
}

func (*entryFile) Close() error {
	return nil
}

func (f *entryFile) Stat() (vfs.FileInfo, error) {
	return entryInfo{
		name: f.entry.Name,
		size: int64(len(f.entry.Payload)),
	}, nil
}

func (f *entryFile) Name() string {
	return f.entry.Name
}

func (f *entryFile) Readdir(n int) ([]vfs.FileInfo, error) {
	return nil, syscall.EOPNOTSUPP
}

func (f *entryFile) Read(p []byte) (int, error) {
	if f.offset >= int64(len(f.entry.Payload)) {
		return 0, io.EOF
	}

	n := copy(p, f.entry.Payload[f.offset:])
	f.offset += int64(n)

	return n, nil
}

func (f *entryFile) Write([]byte) (int, error) {
	return 0, syscall.EPERM
}

func (f *entryFile) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		newOffset = int64(len(f.entry.Payload)) + offset
	default:
		return 0, syscall.EINVAL
	}

	if newOffset < 0 {
		return 0, syscall.EINVAL
	}

	f.offset = newOffset

	return f.offset, nil
}

func (f *entryFile) Truncate(int64) error {
	return syscall.EPERM
}

func (f *entryFile) WriteAt([]byte, int64) (int, error) {
	return 0, syscall.EPERM
}

func (f *entryFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.entry.Payload)) {
		return 0, io.EOF
	}

	n := copy(p, f.entry.Payload[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

type entryInfo struct {
	name string
	size int64
}

func (fi entryInfo) Name() string {
	return fi.name
}

func (fi entryInfo) Size() int64 {
	return fi.size
}

func (fi entryInfo) Mode() os.FileMode {
	return 0o644
}

func (fi entryInfo) ModTime() time.Time {
	return time.Time{}
}

func (fi entryInfo) IsDir() bool {
	return false
}

func (fi entryInfo) Sys() any {
	return nil
}

func (fi entryInfo) NumLinks() uint64 {
	return 1
}

func (fi entryInfo) Extended() (vfs.Attributes, error) {
	return vfs.Attributes{}, nil
}

func (fi entryInfo) Permissions() (*vfs.Permissions, error) {
	return &vfs.Permissions{
		Read:             true,
		GetExtendedAttrs: true,
	}, nil
}

func (fi entryInfo) Uid() uint32 { //nolint:staticcheck
	return 0
}

func (fi entryInfo) Gid() uint32 { //nolint:staticcheck
	return 0
}
