package iofs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"syscall"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/emptyfs"
)

type File struct {
	FS     fs.FS
	File   fs.File
	Path   string
	Offset int64
	sync.Mutex
}

func (f *File) Stat() (vfs.FileInfo, error) {
	if f.Path == "/" {
		return emptyfs.EmptyDirStat{}, nil
	}

	stat, err := f.File.Stat()
	if err != nil {
		return nil, err
	}

	return &FileInfo{stat}, nil
}

func (f *File) Name() string {
	return f.Path
}

func (f *File) Readdir(n int) ([]vfs.FileInfo, error) {
	rdf, ok := f.File.(fs.ReadDirFile)
	if !ok {
		return nil, syscall.ENOTDIR
	}

	entries, err := rdf.ReadDir(n)
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

	return out, nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.Lock()
	defer f.Unlock()

	return f.seek(offset, whence)
}

func (f *File) seek(offset int64, whence int) (int64, error) {
	if seeker, ok := f.File.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}

	switch whence {
	case io.SeekCurrent:
		offset += f.Offset
	case io.SeekEnd:
		fi, err := f.File.Stat()
		if err != nil {
			return 0, err
		}

		offset += fi.Size()
	}

	if offset >= f.Offset {
		err := f.discard(offset - f.Offset)

		return f.Offset, err
	}

	// Need to reopen file
	err := f.File.Close()
	if err != nil {
		return 0, err
	}

	f.File, err = f.FS.Open(StripSlash(f.Path))
	if err != nil {
		return 0, err
	}

	err = f.discard(offset)

	return f.Offset, err
}

func (f *File) discard(n int64) error {
	if n < 0 {
		return io.EOF
	}

	buf := make([]byte, 1024)

	for n > 0 {
		if n < 1024 {
			buf = buf[:n]
		}

		_, err := f.Read(buf)
		if err != nil {
			return err
		}

		n -= int64(len(buf))
	}

	return nil
}

func (f *File) Truncate(int64) error {
	return os.ErrPermission
}

func (f *File) Write(p []byte) (int, error) {
	return 0, os.ErrPermission
}

func (f *File) Read(p []byte) (int, error) {
	f.Lock()
	defer f.Unlock()

	n, err := f.File.Read(p)

	f.Offset += int64(n)

	return n, err
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	f.Lock()
	defer f.Unlock()

	current := f.Offset

	_, err := f.seek(off, io.SeekStart)
	if err != nil {
		return 0, err
	}

	n, err := f.File.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}

	_, err = f.seek(current, io.SeekStart)

	return n, err
}

func (f *File) WriteAt(p []byte, off int64) (int, error) {
	return 0, os.ErrPermission
}

func (f *File) Close() error {
	return f.File.Close()
}
