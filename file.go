package vfs

import (
	"io"
	"os"

	"github.com/kuleuven/vfs/io/readerat"
	"github.com/kuleuven/vfs/io/writerat"
)

func ReadFile(fs FS, path string) (io.ReadSeekCloser, error) {
	readerAt, err := fs.FileRead(path)
	if err != nil {
		return nil, err
	}

	reader := readerat.Reader(readerAt, 0, -1)

	return struct {
		io.ReadSeeker
		io.Closer
	}{
		ReadSeeker: reader,
		Closer:     readerAt,
	}, nil
}

func WriteFile(fs FS, path string) (io.WriteCloser, error) {
	writerAt, err := fs.FileWrite(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, err
	}

	writer := writerat.Writer(writerAt, 0, -1)

	return struct {
		io.Writer
		io.Closer
	}{
		Writer: writer,
		Closer: writerAt,
	}, nil
}

func WriteFileExclusive(fs FS, path string) (io.WriteCloser, error) {
	writerAt, err := fs.FileWrite(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return nil, err
	}

	writer := writerat.Writer(writerAt, 0, -1)

	return struct {
		io.Writer
		io.Closer
	}{
		Writer: writer,
		Closer: writerAt,
	}, nil
}

func NopWriterAt(r ReaderAt) WriterAtReaderAt {
	return &nopWriterAt{
		ReaderAt: r,
	}
}

type nopWriterAt struct {
	ReaderAt
}

func (r *nopWriterAt) WriteAt([]byte, int64) (int, error) {
	return 0, os.ErrPermission
}

func NopReaderAt(w WriterAt) WriterAtReaderAt {
	return &nopReaderAt{
		WriterAt: w,
	}
}

type nopReaderAt struct {
	WriterAt
}

func (r *nopReaderAt) ReadAt([]byte, int64) (int, error) {
	return 0, os.ErrPermission
}
