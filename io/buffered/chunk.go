package buffered

import (
	"errors"
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
)

type Chunk struct {
	offset  int64
	buf     []byte      // The actual buffer
	slices  map[int]int // The start and lengths of each slice. These can overlap if Write is called for overlapping parts.
	read    int         // Number of bytes read
	written int         // Number of bytes written
}

func newChunk(offset int64, size int) *Chunk {
	return &Chunk{
		offset: offset,
		buf:    make([]byte, size),
		slices: map[int]int{},
	}
}

var ErrInvalidOffset = errors.New("invalid offset")

var ErrNoDataAtOffset = errors.New("chunk has no data slice at this offset")

// WriteAt the payload to the chunk and return the number of bytes written.
// The start offset must be inside the chunk. If the payload is larger than the chunk size, only the first part until the chunk end is written.
func (c *Chunk) WriteAt(payload []byte, offset int64) (int, error) {
	start64 := offset - c.offset

	if start64 < 0 {
		return 0, fmt.Errorf("%w: offset %d is lower than chunk offset %d", ErrInvalidOffset, offset, c.offset)
	}

	if start64 >= int64(len(c.buf)) {
		return 0, fmt.Errorf("%w: offset %d is after than chunk end %d", ErrInvalidOffset, offset, c.offset+int64(len(c.buf))-1)
	}

	// We are sure start fits in an int
	start := int(start64)

	n := copy(c.buf[start:], payload)

	c.written += n

	// N defines the slice length to be stored in c.slices
	N := n

	// If our end position is known, eat the current slice that it contains
	if pos := c.has(start + n); pos > start {
		N = c.slices[pos] + (pos - start)

		delete(c.slices, pos)
	} else if pos <= start && pos >= 0 {
		// The written bytes fall completely in an already known slice
		return n, nil
	}

	// Try to concatenate to an existing slice if possible
	if pos := c.has(start - 1); pos >= 0 {
		c.slices[pos] = maxInt(c.slices[pos], (start-pos)+N)
	} else {
		c.slices[start] = N
	}

	return n, nil
}

func maxInt(a, b int) int {
	if a < b {
		return b
	}

	return a
}

// ReadAt payload from the chunk.
func (c *Chunk) ReadAt(payload []byte, offset int64) (int, error) {
	start64 := offset - c.offset

	if start64 < 0 {
		return 0, fmt.Errorf("%w: offset %d is lower than chunk offset %d", ErrInvalidOffset, offset, c.offset)
	}

	if start64 >= int64(len(c.buf)) {
		return 0, fmt.Errorf("%w: offset %d is after than chunk end %d", ErrInvalidOffset, offset, c.offset+int64(len(c.buf))-1)
	}

	// We are sure start fits in an int
	start := int(start64)

	// number of bytes, starting at offset, that has been copied
	var copied int

outer:
	for {
		for pos, n := range c.slices {
			if pos > start || start >= pos+n {
				continue
			}

			copied += copy(payload, c.buf[start:pos+n])

			if copied < n { // We reached the end of the payload []byte
				c.read += copied
				return copied, nil
			}

			start += copied

			continue outer
		}

		if copied > 0 {
			c.read += copied
			return copied, nil
		}

		return 0, ErrNoDataAtOffset
	}
}

// Contains checks whether the given offset falls in the chunk, but not whether the chunk knowns the actual byte at that offset.
func (c *Chunk) Contains(offset int64) bool {
	start64 := offset - c.offset

	if start64 < 0 {
		return false
	}

	return start64 < int64(len(c.buf))
}

// Overlaps checks whether the given offset range overlaps with the chunk.
func (c *Chunk) Overlaps(offset int64, length int) bool {
	start64 := offset - c.offset          // first byte of range relative to chunk
	last64 := start64 + int64(length) - 1 // last byte of range relative to chunk

	if last64 < 0 {
		return false
	}

	if start64 >= int64(len(c.buf)) {
		return false
	}

	return true
}

// SizeAt returns the number of known bytes starting at the given offset.
func (c *Chunk) SizeAt(offset int64) int {
	start64 := offset - c.offset

	if start64 < 0 {
		return 0
	}

	if start64 >= int64(len(c.buf)) {
		return 0
	}

	start := int(start64)

	return c.sizeAt(start)
}

// sizeAt is like SizeAt but for relative offsets.
func (c *Chunk) sizeAt(relOffset int) int {
	var knownBytes int

outer:
	for {
		for pos, n := range c.slices {
			if pos <= relOffset && relOffset < pos+n {
				// Yes, I know this
				knownBytes += pos + n - relOffset
				relOffset += knownBytes

				continue outer
			}
		}

		return knownBytes
	}
}

// has returns the relative start offset of a slice that contains the given offset.
// If the offset is not found, -1 is returned.
func (c *Chunk) has(relOffset int) int {
	if relOffset < 0 {
		return -1
	}

	for pos, n := range c.slices {
		if pos <= relOffset && relOffset < pos+n {
			return pos
		}
	}

	return -1
}

// Slices returns the list of slices in the chunk, indexed by the absolute offset.
// These slices may be adjecent. Use SizeAt to know how many bytes are known from
// a given offset onwards.
func (c *Chunk) Slices() map[int64][]byte {
	m := map[int64][]byte{}

	for pos, n := range c.slices {
		m[c.offset+int64(pos)] = c.buf[pos : pos+n]
	}

	return m
}

// Reset empties the chunk.
func (c *Chunk) Reset(offset int64) {
	c.offset = offset
	c.slices = map[int]int{}
	c.read = 0
	c.written = 0
}

// Fill the chunk using the given io.ReaderAt reader
func (c *Chunk) FromReader(reader io.ReaderAt) (int, error) {
	c.slices = map[int]int{}

	var (
		n, t int
		err  error
	)

	// Do a full read of the chunk, and account for the case that ReadAt returns less than the chunk size
	for err == nil && t < len(c.buf) {
		n, err = reader.ReadAt(c.buf[t:], c.offset+int64(t))
		if errors.Is(err, io.EOF) && n > 0 {
			err = nil
		}

		t += n
	}

	if errors.Is(err, io.EOF) && t > 0 {
		err = nil
	}

	if err == nil {
		c.slices[0] = t
	}

	return t, err
}

func (c *Chunk) WriteTo(w io.WriterAt) (int, error) {
	var written int

	for offset, buf := range c.Slices() {
		logrus.Tracef("Writing [%d, %d]", offset, offset+int64(len(buf))-1)

		n, err := w.WriteAt(buf, offset)
		if err != nil {
			return written, err
		}

		written += n
	}

	return written, nil
}

// IsFull checks whether the chunk is completely filled
func (c *Chunk) IsFull() bool {
	// If we didn't write enough bytes, it is definitely not full.
	if c.written < len(c.buf) {
		return false
	}

	return c.sizeAt(0) >= len(c.buf)
}

// Written returns the number of written bytes
func (c *Chunk) Written() int {
	return c.written
}

// Read returns the number of read bytes
func (c *Chunk) Read() int {
	return c.read
}
