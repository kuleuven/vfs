package sysfs

import (
	"crypto"
	"io"
	"os"
	"strings"

	"github.com/kuleuven/vfs"
)

func New(top Directory) *FS {
	return &FS{
		top: top,
	}
}

type FS struct {
	vfs.NotImplementedRootFS
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

	h := algorithm.New()

	if _, err := io.Copy(h, entry.File()); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}
