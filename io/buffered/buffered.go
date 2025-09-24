package buffered

import (
	"io"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

type BufferedReaderAt struct {
	ReaderAt  io.ReaderAt
	ChunkSize int
	MaxChunks int
	chunks    []*Chunk
	sync.Mutex
}

func (b *BufferedReaderAt) ReadAt(buf []byte, off int64) (int, error) {
	var (
		n, seen int
		err     error
	)

	for err == nil && seen < len(buf) {
		n, err = b.ReadChunkAt(buf[seen:], off+int64(seen))
		seen += n
	}

	logrus.Tracef("ReadAt [%d, %d] => %d, %v", off, off+int64(len(buf))-1, seen, err)

	return seen, err
}

// Read a single chunk at the given offset. The offset must not be aligned to the chunk start,
// but the bytes read will be aligned at the end of the chunk. Usually, the buffer will be smaller
// than the chunk size, but as clients decide the buffer size it can be larger.
func (b *BufferedReaderAt) ReadChunkAt(buf []byte, off int64) (int, error) {
	b.Lock()

	defer b.Unlock()

	// Read from the current chunks if possible
	for _, chunk := range b.chunks {
		if !chunk.Contains(off) {
			continue
		}

		return returnEOF(chunk.ReadAt(buf, off))
	}

	// Create a new chunk or reuse a chunk
	var chunk *Chunk

	chunkOffset := (off / int64(b.ChunkSize)) * int64(b.ChunkSize)

	if len(b.chunks) < b.MaxChunks {
		chunk = newChunk(chunkOffset, b.ChunkSize)
	} else {
		chunk = b.chunks[0]
		b.chunks = b.chunks[1:]

		chunk.Reset(chunkOffset)
	}

	// Populate the chunk
	logrus.Tracef("Reading [%d,%d]", chunkOffset, chunkOffset+int64(b.ChunkSize)-1)

	n, err := chunk.FromReader(b.ReaderAt)
	if err != nil {
		return n, err
	}

	b.chunks = append(b.chunks, chunk)

	// Return result from new chunk
	return returnEOF(chunk.ReadAt(buf, off))
}

// Invalidate invalidates all chunks that intersect with the given range.
func (b *BufferedReaderAt) Invalidate(off int64, length int) {
	b.Lock()

	defer b.Unlock()

	kept := []*Chunk{}

	// Read from the current chunks if possible
	for _, chunk := range b.chunks {
		if !chunk.Overlaps(off, length) {
			kept = append(kept, chunk)
		}
	}

	b.chunks = kept
}

func returnEOF(n int, err error) (int, error) {
	if err == ErrNoDataAtOffset {
		return n, io.EOF
	}

	return n, err
}

// Close closes the underlying reader.
func (b *BufferedReaderAt) Close() error {
	b.Lock()

	defer b.Unlock()

	if closer, ok := b.ReaderAt.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

type BufferedWriterAt struct {
	WriterAt  io.WriterAt
	ChunkSize int
	MaxChunks int
	chunks    []*Chunk
	sync.Mutex
}

func (b *BufferedWriterAt) WriteAt(buf []byte, off int64) (int, error) {
	var (
		n, seen int
		err     error
	)

	for err == nil && seen < len(buf) {
		n, err = b.WriteChunkAt(buf[seen:], off+int64(seen))
		seen += n
	}

	logrus.Tracef("WriteAt [%d, %d] => %d, %v", off, off+int64(len(buf))-1, seen, err)

	return seen, err
}

// Write a single chunk at the given offset. The offset must not be aligned to the chunk start,
// but the bytes written will be aligned at the end of the chunk.
func (b *BufferedWriterAt) WriteChunkAt(buf []byte, off int64) (int, error) {
	b.Lock()

	defer b.Unlock()

	// Write to the current chunks if possible
	for _, chunk := range b.chunks {
		if !chunk.Contains(off) {
			continue
		}

		return chunk.WriteAt(buf, off)
	}

	// Create a new chunk if needed
	chunkOffset := (off / int64(b.ChunkSize)) * int64(b.ChunkSize)

	chunk, err := b.freeChunk(chunkOffset)
	if err != nil {
		return 0, err
	}

	// Append chunk to current chunk lists
	b.chunks = append(b.chunks, chunk)

	return chunk.WriteAt(buf, off)
}

// Create a new chunk or reuse the chunk that has the most bytes written to it.
func (b *BufferedWriterAt) freeChunk(offset int64) (*Chunk, error) {
	if len(b.chunks) < b.MaxChunks {
		return newChunk(offset, b.ChunkSize), nil
	}

	// Discard largest chunk
	var (
		largestChunk *Chunk
		p            int
	)

	for i, chunk := range b.chunks {
		if largestChunk == nil || largestChunk.Written() < chunk.Written() {
			largestChunk = chunk
			p = i
		}
	}

	if largestChunk == nil {
		return newChunk(offset, b.ChunkSize), nil
	}

	_, err := largestChunk.WriteTo(b.WriterAt)
	if err != nil {
		return nil, err
	}

	b.chunks = append(b.chunks[:p], b.chunks[p+1:]...)

	largestChunk.Reset(offset)

	return largestChunk, nil
}

// Close flushes all chunks and closes the underlying writer.
func (b *BufferedWriterAt) Close() error {
	b.Lock()

	defer b.Unlock()

	errs := []error{}

	for _, chunk := range b.chunks {
		_, err := chunk.WriteTo(b.WriterAt)

		errs = append(errs, err)
	}

	if closer, ok := b.WriterAt.(io.Closer); ok {
		errs = append(errs, closer.Close())
	}

	return MultipleError(errs...)
}

func MultipleError(errs ...error) error {
	var realErrs []error

	for _, err := range errs {
		if err != nil {
			realErrs = append(realErrs, err)
		}
	}

	switch len(realErrs) {
	case 0:
		return nil
	case 1:
		return realErrs[0]
	default:
		return MultipleErrors(realErrs)
	}
}

type MultipleErrors []error

func (m MultipleErrors) Error() string {
	var errStrs []string

	for _, err := range m {
		errStrs = append(errStrs, err.Error())
	}

	return "multiple errors: " + strings.Join(errStrs, ", ")
}
