package buffered

import (
	"io"
	"sync"
)

type BackgroundReader struct {
	ReaderAt  io.ReaderAt
	ChunkSize int
	chunk     *Chunk
	readErr   error
	sync.WaitGroup
}

func (c *BackgroundReader) ReadAt(buf []byte, offset int64) (int, error) {
	c.Wait()

	var (
		n   int
		err error
	)

	if c.chunk != nil && c.chunk.Contains(offset) {
		if c.readErr != nil && c.readErr != io.EOF {
			return 0, c.readErr
		}

		n, err = c.chunk.ReadAt(buf, offset)
	} else {
		n, err = c.ReaderAt.ReadAt(buf, offset)
	}

	if err != nil {
		return n, err
	}

	if c.chunk == nil {
		c.chunk = newChunk(offset+int64(len(buf)), c.ChunkSize)
	} else {
		c.chunk.Reset(offset + int64(len(buf)))
	}

	c.Add(1)

	go func() {
		defer c.Done()

		_, c.readErr = c.chunk.FromReader(c.ReaderAt)
	}()

	return n, nil
}

// Close closes the underlying reader.
func (c *BackgroundReader) Close() error {
	c.Wait()

	if closer, ok := c.ReaderAt.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

type BackgroundWriter struct {
	WriterAt  io.WriterAt
	ChunkSize int
	chunk     *Chunk
	writeErr  error
	sync.WaitGroup
}

func (c *BackgroundWriter) WriteAt(buf []byte, offset int64) (int, error) {
	c.Wait()

	if c.writeErr != nil {
		return 0, c.writeErr
	}

	if c.chunk == nil {
		c.chunk = newChunk(offset, c.ChunkSize)
	} else {
		c.chunk.Reset(offset)
	}

	n, err := c.chunk.WriteAt(buf, offset)
	if err != nil {
		return n, err
	}

	c.Add(1)

	go func() {
		defer c.Done()

		_, c.writeErr = c.chunk.WriteTo(c.WriterAt)
	}()

	if n < len(buf) {
		return n, io.ErrShortWrite
	}

	return n, nil
}

func (c *BackgroundWriter) Close() error {
	c.Wait()

	errs := []error{
		c.writeErr,
	}

	if closer, ok := c.WriterAt.(io.Closer); ok {
		errs = append(errs, closer.Close())
	}

	return MultipleError(errs...)
}
