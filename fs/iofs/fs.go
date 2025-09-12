package iofs

import (
	"io"
	"io/fs"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/emptyfs"
	"github.com/kuleuven/vfs/io/readerat"
)

func New(f fs.FS) *FS {
	return &FS{
		FS: f,
	}
}

type FS struct {
	vfs.NotImplementedFS
	FS fs.FS
}

var _ vfs.FS = &FS{}

func (w *FS) Stat(path string) (vfs.FileInfo, error) {
	if path == "/" {
		return emptyfs.EmptyDirStat{}, nil
	}

	fi, err := fs.Stat(w.FS, StripSlash(path))
	if err != nil {
		return nil, err
	}

	return &FileInfo{fi}, nil
}

func (w *FS) List(path string) (vfs.ListerAt, error) {
	entries, err := fs.ReadDir(w.FS, StripSlash(path))
	if err != nil {
		return nil, err
	}

	out := make([]vfs.FileInfo, 0, len(entries))

	for _, entry := range entries {
		fi, err := entry.Info()
		if err != nil {
			return nil, err
		}

		out = append(out, &FileInfo{fi})
	}

	return vfs.FileInfoListerAt(out), nil
}

func (w *FS) Open(path string) (vfs.File, error) {
	f, err := w.FS.Open(StripSlash(path))
	if err != nil {
		return nil, err
	}

	return &File{
		FS:   w.FS,
		File: f,
		Path: path,
	}, nil
}

func (w *FS) FileRead(path string) (vfs.ReaderAt, error) {
	f, err := w.Open(path)
	if err != nil {
		return nil, err
	}

	if r, ok := f.(vfs.ReaderAt); ok {
		return r, nil
	}

	return &struct {
		io.ReaderAt
		io.Closer
	}{
		ReaderAt: readerat.ReaderAt(f),
		Closer:   f,
	}, nil
}

func StripSlash(path string) string {
	if path[0] == '/' {
		path = path[1:]
	}

	if path == "" {
		return "."
	}

	return path
}
