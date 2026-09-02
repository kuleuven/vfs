package sysfs

import (
	"crypto"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kuleuven/vfs"
)

func New(top Directory) *FS {
	return &FS{
		top: top,
	}
}

type FS struct {
	top Directory
}

var _ vfs.RootFS = FS{}

func (e FS) Stat(path string) (vfs.FileInfo, error) {
	path, ok := strings.CutPrefix(path, "/")
	if !ok {
		return nil, os.ErrNotExist
	}

	if obj := e.top.Find(path); obj != nil {
		return obj.Info(), nil
	}

	return nil, os.ErrNotExist
}

func (e FS) Lstat(path string) (vfs.FileInfo, error) {
	return e.Stat(path)
}

func (e FS) List(path string) (vfs.ListerAt, error) {
	path, ok := strings.CutPrefix(path, "/")
	if !ok {
		return nil, os.ErrNotExist
	}

	if dir := e.top.FindDirectory(path); dir != nil {
		return directoryList{directory: *dir}, nil
	}

	return nil, os.ErrNotExist
}

func (e FS) Walk(path string, walkFn vfs.WalkFunc) error {
	return vfs.Walk(walkWrapper{sub: e}, path, walkFn)
}

type walkWrapper struct {
	sub vfs.WalkableFS
}

func (e walkWrapper) Stat(path string) (vfs.FileInfo, error) {
	return e.sub.Stat(path)
}

func (e walkWrapper) List(path string) (vfs.ListerAt, error) {
	return e.sub.List(path)
}

func (e FS) Open(path string) (vfs.File, error) {
	path, ok := strings.CutPrefix(path, "/")
	if !ok {
		return nil, os.ErrNotExist
	}

	if obj := e.top.Find(path); obj != nil {
		return obj.File(), nil
	}

	return nil, os.ErrNotExist
}

func (e FS) OpenFile(path string, flag int, _ os.FileMode) (vfs.File, error) {
	if flag&os.O_WRONLY != 0 || flag&os.O_RDWR != 0 {
		return nil, os.ErrPermission
	}

	return e.Open(path)
}

func (e FS) FileRead(path string) (vfs.ReaderAt, error) {
	file, err := e.Open(path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (FS) FileWrite(path string, flag int) (vfs.WriterAt, error) {
	return nil, os.ErrPermission
}

func (e FS) Handle(path string) ([]byte, error) {
	return []byte(path), nil
}

func (FS) Path(handle []byte) (string, error) {
	return string(handle), nil
}

func (e FS) Checksum(path string, algorithm crypto.Hash) ([]byte, error) {
	path, ok := strings.CutPrefix(path, "/")
	if !ok {
		return nil, os.ErrNotExist
	}

	entry := e.top.FindEntry(path)
	if entry == nil {
		return nil, os.ErrNotExist
	}

	if !algorithm.Available() {
		return nil, os.ErrInvalid
	}

	h := algorithm.New()

	if _, err := io.Copy(h, entry.File()); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func (e FS) Chmod(path string, mode os.FileMode) error {
	return os.ErrPermission
}

func (e FS) Chown(path string, uid, gid int) error {
	return os.ErrPermission
}

func (FS) Chtimes(path string, atime, mtime time.Time) error {
	return os.ErrPermission
}

func (FS) Truncate(path string, size int64) error {
	return os.ErrPermission
}

func (FS) SetExtendedAttr(path, name string, value []byte) error {
	return os.ErrPermission
}

func (FS) UnsetExtendedAttr(path, name string) error {
	return os.ErrPermission
}

func (FS) Rename(oldpath, newpath string) error {
	return os.ErrPermission
}

func (FS) Rmdir(path string) error {
	return os.ErrPermission
}

func (FS) Remove(path string) error {
	return os.ErrPermission
}

func (FS) Mkdir(path string, perm os.FileMode) error {
	return os.ErrPermission
}

func (FS) Close() error {
	return nil
}

func (FS) Link(oldname, newname string) error {
	return os.ErrPermission
}

func (FS) Symlink(target, link string) error {
	return os.ErrPermission
}

func (FS) Readlink(path string) (string, error) {
	return "", os.ErrInvalid
}

func (e FS) RealPath(path string) (string, error) {
	if _, err := e.Stat(path); err != nil {
		return "", err
	}

	return path, nil
}

func (FS) Mount(path string, fs vfs.FS, index byte) error {
	return os.ErrPermission
}

func (FS) SetExtendedAttrs(path string, attrs vfs.Attributes) error {
	return os.ErrPermission
}
