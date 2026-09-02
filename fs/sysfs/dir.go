package sysfs

import (
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/kuleuven/vfs"
)

type Directory struct {
	Name        string
	Directories []Directory
	Entries     []Entry
}

func (d Directory) Info() vfs.FileInfo {
	return directoryInfo{name: d.Name}
}

func (d Directory) File() vfs.File {
	return &directoryFile{
		directory: d,
		offset:    0,
	}
}

type Object interface {
	Info() vfs.FileInfo
	File() vfs.File
}

func (d Directory) Find(name string) Object {
	if name == "" {
		return d
	}

	parts := strings.SplitN(name, "/", 2)

	for _, subdir := range d.Directories {
		if subdir.Name != parts[0] {
			continue
		}

		if len(parts) < 2 {
			parts = append(parts, "")
		}

		return subdir.Find(parts[1])
	}

	if len(parts) > 1 {
		return nil
	}

	for _, entry := range d.Entries {
		if entry.Name == parts[0] {
			return entry
		}
	}

	return nil
}

func (d Directory) FindDirectory(name string) *Directory {
	if name == "" {
		return &d
	}

	parts := strings.SplitN(name, "/", 2)

	for _, subdir := range d.Directories {
		if subdir.Name != parts[0] {
			continue
		}

		if len(parts) < 2 {
			parts = append(parts, "")
		}

		return subdir.FindDirectory(parts[1])
	}

	return nil
}

func (d Directory) FindEntry(name string) *Entry {
	if name == "" {
		return nil
	}

	parts := strings.SplitN(name, "/", 2)

	if len(parts) > 1 {
		dir := d.FindDirectory(parts[0])
		if dir == nil {
			return nil
		}

		return dir.FindEntry(parts[1])
	}

	for _, entry := range d.Entries {
		if entry.Name == parts[0] {
			return &entry
		}
	}

	return nil
}

type directoryFile struct {
	directory Directory
	offset    int64
}

func (d *directoryFile) Close() error {
	return nil
}

func (d *directoryFile) Stat() (vfs.FileInfo, error) {
	return directoryInfo{name: d.directory.Name}, nil
}

func (d *directoryFile) Name() string {
	return d.directory.Name
}

func (d *directoryFile) Readdir(n int) ([]vfs.FileInfo, error) {
	if d.offset >= int64(len(d.directory.Entries)+len(d.directory.Directories)) {
		return nil, io.EOF
	}

	var infos []vfs.FileInfo

	for i := d.offset; i < int64(len(d.directory.Entries)+len(d.directory.Directories)) && (n <= 0 || len(infos) < n); i++ {
		var info vfs.FileInfo

		if i < int64(len(d.directory.Entries)) {
			info = d.directory.Entries[i].Info()
		} else {
			info = d.directory.Directories[i-int64(len(d.directory.Entries))].Info()
		}

		infos = append(infos, info)
		d.offset++
	}

	return infos, nil
}

func (*directoryFile) Read([]byte) (int, error) {
	return 0, syscall.EOPNOTSUPP
}

func (*directoryFile) Write([]byte) (int, error) {
	return 0, syscall.EOPNOTSUPP
}

func (*directoryFile) Seek(int64, int) (int64, error) {
	return 0, syscall.EOPNOTSUPP
}

func (*directoryFile) Truncate(int64) error {
	return syscall.EOPNOTSUPP
}

func (*directoryFile) WriteAt([]byte, int64) (int, error) {
	return 0, syscall.EOPNOTSUPP
}

func (*directoryFile) ReadAt([]byte, int64) (int, error) {
	return 0, syscall.EOPNOTSUPP
}

type directoryInfo struct {
	name string
}

func (fi directoryInfo) Name() string {
	return fi.name
}

func (fi directoryInfo) Size() int64 {
	return 0
}

func (fi directoryInfo) Mode() os.FileMode {
	return 0o755 | os.ModeDir
}

func (fi directoryInfo) ModTime() time.Time {
	return time.Time{}
}

func (fi directoryInfo) IsDir() bool {
	return true
}

func (fi directoryInfo) Sys() any {
	return nil
}

func (fi directoryInfo) NumLinks() uint64 {
	return 1
}

func (fi directoryInfo) Extended() (vfs.Attributes, error) {
	return vfs.Attributes{}, nil
}

func (fi directoryInfo) Permissions() (*vfs.Permissions, error) {
	return &vfs.Permissions{
		Read:             true,
		GetExtendedAttrs: true,
	}, nil
}

func (fi directoryInfo) Uid() uint32 { //nolint:staticcheck
	return 0
}

func (fi directoryInfo) Gid() uint32 { //nolint:staticcheck
	return 0
}

type directoryList struct {
	directory Directory
}

func (directoryList) Close() error {
	return nil
}

func (d directoryList) ListAt(buf []vfs.FileInfo, offset int64) (int, error) {
	if offset >= int64(len(d.directory.Entries)+len(d.directory.Directories)) {
		return 0, io.EOF
	}

	n := 0

	for i := offset; i < int64(len(d.directory.Entries)+len(d.directory.Directories)) && n < len(buf); i++ {
		var info vfs.FileInfo

		if i < int64(len(d.directory.Entries)) {
			info = d.directory.Entries[i].Info()
		} else {
			info = d.directory.Directories[i-int64(len(d.directory.Entries))].Info()
		}

		buf[n] = info
		n++
	}

	if n < len(buf) {
		return n, io.EOF
	}

	return n, nil
}
