package readerat

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
)

// Mock ReaderAt for testing
type mockReaderAt struct {
	data []byte
}

func (m *mockReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(m.data)) {
		return 0, io.EOF
	}

	n := copy(p, m.data[off:])
	if off+int64(n) >= int64(len(m.data)) {
		return n, io.EOF
	}

	return n, nil
}

// Mock ReadSeeker for testing
type mockReadSeeker struct {
	data   []byte
	offset int64
}

func (m *mockReadSeeker) Read(p []byte) (int, error) {
	if m.offset >= int64(len(m.data)) {
		return 0, io.EOF
	}

	n := copy(p, m.data[m.offset:])
	m.offset += int64(n)

	if m.offset >= int64(len(m.data)) {
		return n, io.EOF
	}

	return n, nil
}

func (m *mockReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.offset = offset
	case io.SeekCurrent:
		m.offset += offset
	case io.SeekEnd:
		m.offset = int64(len(m.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if m.offset < 0 {
		m.offset = 0
	} else if m.offset > int64(len(m.data)) {
		m.offset = int64(len(m.data))
	}

	return m.offset, nil
}

func TestReader(t *testing.T) { //nolint:funlen,gocognit
	data := []byte("Hello, world! This is a test.")
	mockRA := &mockReaderAt{data: data}

	t.Run("Basic reading", func(t *testing.T) {
		reader := Reader(mockRA, 0, int64(len(data)))

		buf := make([]byte, 5)

		n, err := reader.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != 5 {
			t.Fatalf("expected 5 bytes read, got %d", n)
		}

		if string(buf) != "Hello" {
			t.Fatalf("expected 'Hello', got '%s'", string(buf))
		}

		// Check offset
		if reader.Offset() != 5 {
			t.Fatalf("expected offset 5, got %d", reader.Offset())
		}
	})

	t.Run("Reading with offset", func(t *testing.T) {
		reader := Reader(mockRA, 7, int64(len(data)-7))

		buf := make([]byte, 5)

		n, err := reader.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != 5 {
			t.Fatalf("expected 5 bytes read, got %d", n)
		}

		if string(buf) != "world" {
			t.Fatalf("expected 'world', got '%s'", string(buf))
		}
	})

	t.Run("Reading beyond data", func(t *testing.T) {
		reader := Reader(mockRA, 0, int64(len(data)))

		// Read all data
		buf := make([]byte, len(data)+10)

		n, err := reader.Read(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}

		if n != len(data) {
			t.Fatalf("expected %d bytes read, got %d", len(data), n)
		}
	})

	t.Run("Multiple reads", func(t *testing.T) {
		reader := Reader(mockRA, 0, int64(len(data)))

		// First read
		buf1 := make([]byte, 7)

		n1, err := reader.Read(buf1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n1 != 7 || string(buf1) != "Hello, " {
			t.Fatalf("unexpected first read: %d bytes, '%s'", n1, string(buf1))
		}

		// Second read
		buf2 := make([]byte, 6)

		n2, err := reader.Read(buf2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n2 != 6 || string(buf2) != "world!" {
			t.Fatalf("unexpected second read: %d bytes, '%s'", n2, string(buf2))
		}
	})
}

func TestReaderSeek(t *testing.T) { //nolint:funlen,gocognit
	data := []byte("Hello, World! This is a test.")
	mockRA := &mockReaderAt{data: data}

	t.Run("Seek from start", func(t *testing.T) {
		reader := Reader(mockRA, 0, int64(len(data)))

		offset, err := reader.Seek(7, io.SeekStart)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != 7 {
			t.Fatalf("expected offset 7, got %d", offset)
		}

		buf := make([]byte, 5)

		_, err = reader.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if string(buf) != "World" {
			t.Fatalf("expected 'World', got '%s'", string(buf))
		}
	})

	t.Run("Seek from current", func(t *testing.T) {
		reader := Reader(mockRA, 5, int64(len(data)-5))

		// First read to advance position
		buf := make([]byte, 2)
		reader.Read(buf)

		// Seek relative to current position
		offset, err := reader.Seek(2, io.SeekCurrent)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != 9 {
			t.Fatalf("expected offset 9, got %d", offset)
		}

		buf = make([]byte, 5)

		_, err = reader.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if string(buf) != "orld!" {
			t.Fatalf("expected 'orld!', got '%s'", string(buf))
		}
	})

	t.Run("Seek from end", func(t *testing.T) {
		reader := Reader(mockRA, 0, int64(len(data)))

		offset, err := reader.Seek(-5, io.SeekEnd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := int64(len(data)) - 5
		if offset != expected {
			t.Fatalf("expected offset %d, got %d", expected, offset)
		}

		buf := make([]byte, 5)

		_, err = reader.Read(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}

		if string(buf) != "test." {
			t.Fatalf("expected 'test.', got '%s'", string(buf))
		}
	})

	t.Run("Seek from end with unknown length", func(t *testing.T) {
		reader := Reader(mockRA, 0, -1) // Unknown length

		_, err := reader.Seek(-5, io.SeekEnd)
		if !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("expected ErrInvalid, got %v", err)
		}
	})
}

func TestReaderAt(t *testing.T) { //nolint:funlen,gocognit
	data := []byte("Hello, World! This is a test.")

	t.Run("Basic ReaderAt functionality", func(t *testing.T) {
		mockRS := &mockReadSeeker{data: data, offset: 0}
		readerAt := ReaderAt(mockRS)

		buf := make([]byte, 5)

		n, err := readerAt.ReadAt(buf, 7)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != 5 {
			t.Fatalf("expected 5 bytes read, got %d", n)
		}

		if string(buf) != "World" {
			t.Fatalf("expected 'World', got '%s'", string(buf))
		}
	})

	t.Run("ReadAt at different positions", func(t *testing.T) {
		mockRS := &mockReadSeeker{data: data, offset: 0}
		readerAt := ReaderAt(mockRS)

		// Read from beginning
		buf1 := make([]byte, 5)

		n1, err := readerAt.ReadAt(buf1, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if string(buf1) != "Hello" {
			t.Fatalf("expected 'Hello', got '%s'", string(buf1))
		}

		// Read from middle
		buf2 := make([]byte, 5)

		n2, err := readerAt.ReadAt(buf2, 14)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if string(buf2) != "This " {
			t.Fatalf("expected 'This ', got '%s'", string(buf2))
		}

		// Verify positions don't affect each other
		if n1 != 5 || n2 != 5 {
			t.Fatalf("unexpected read lengths: %d, %d", n1, n2)
		}
	})

	t.Run("ReadAt beyond end", func(t *testing.T) {
		mockRS := &mockReadSeeker{data: data, offset: 0}
		readerAt := ReaderAt(mockRS)

		buf := make([]byte, 10)

		n, err := readerAt.ReadAt(buf, int64(len(data)-3))
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}

		if n != 3 {
			t.Fatalf("expected 3 bytes read, got %d", n)
		}

		if string(buf[:n]) != "st." {
			t.Fatalf("expected 'st.', got '%s'", string(buf[:n]))
		}
	})

	t.Run("Sequential ReadAt calls", func(t *testing.T) {
		mockRS := &mockReadSeeker{data: data, offset: 0}
		readerAt := ReaderAt(mockRS)

		// First call
		buf1 := make([]byte, 5)
		readerAt.ReadAt(buf1, 0)

		// Second call at same position
		buf2 := make([]byte, 5)
		readerAt.ReadAt(buf2, 0)

		if !bytes.Equal(buf1, buf2) {
			t.Fatalf("sequential reads at same position should return same data")
		}
	})
}

func TestConcurrentAccess(t *testing.T) { //nolint:funlen,gocognit
	data := []byte(strings.Repeat("Hello, World! ", 1000))
	mockRA := &mockReaderAt{data: data}
	reader := Reader(mockRA, 0, int64(len(data)))

	t.Run("Concurrent reads", func(t *testing.T) {
		const numGoroutines = 10

		const numReads = 100

		var wg sync.WaitGroup

		errorch := make(chan error, numGoroutines)

		for range numGoroutines {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range numReads {
					buf := make([]byte, 5)

					_, err := reader.Read(buf)
					if err != nil && !errors.Is(err, io.EOF) {
						errorch <- err
						return
					}
				}
			}()
		}

		wg.Wait()
		close(errorch)

		// Check for any errors
		for err := range errorch {
			t.Fatalf("concurrent read error: %v", err)
		}

		// Verify final offset makes sense
		finalOffset := reader.Offset()
		if finalOffset > int64(len(data)) {
			t.Fatalf("offset exceeded data length: %d > %d", finalOffset, len(data))
		}
	})

	t.Run("Concurrent seeks", func(t *testing.T) {
		const numGoroutines = 10

		var wg sync.WaitGroup

		errorch := make(chan error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				// Seek to different positions
				offset := int64(id * 10)

				_, err := reader.Seek(offset, io.SeekStart)
				if err != nil {
					errorch <- err
					return
				}

				// Read after seek
				buf := make([]byte, 5)

				_, err = reader.Read(buf)
				if err != nil && !errors.Is(err, io.EOF) {
					errorch <- err
					return
				}
			}(i)
		}

		wg.Wait()
		close(errorch)

		// Check for any errors
		for err := range errorch {
			t.Fatalf("concurrent seek error: %v", err)
		}
	})
}

func TestReaderAtConcurrentAccess(t *testing.T) {
	data := []byte(strings.Repeat("Hello, World! ", 1000))
	mockRS := &mockReadSeeker{data: data, offset: 0}
	readerAt := ReaderAt(mockRS)

	t.Run("Concurrent ReadAt calls", func(t *testing.T) {
		const numGoroutines = 10

		const numReads = 100

		var wg sync.WaitGroup

		errorch := make(chan error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				for j := range numReads {
					buf := make([]byte, 5)
					offset := int64((id*numReads + j) % (len(data) - 5))

					_, err := readerAt.ReadAt(buf, offset)
					if err != nil && !errors.Is(err, io.EOF) {
						errorch <- err
						return
					}
				}
			}(i)
		}

		wg.Wait()
		close(errorch)

		// Check for any errors
		for err := range errorch {
			t.Fatalf("concurrent ReadAt error: %v", err)
		}
	})
}

func TestEdgeCases(t *testing.T) { //nolint:funlen
	t.Run("Zero-length read", func(t *testing.T) {
		data := []byte("Hello")
		mockRA := &mockReaderAt{data: data}
		reader := Reader(mockRA, 0, int64(len(data)))

		buf := make([]byte, 0)

		n, err := reader.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != 0 {
			t.Fatalf("expected 0 bytes read, got %d", n)
		}
	})

	t.Run("Empty data", func(t *testing.T) {
		mockRA := &mockReaderAt{data: []byte{}}
		reader := Reader(mockRA, 0, 0)

		buf := make([]byte, 5)

		n, err := reader.Read(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}

		if n != 0 {
			t.Fatalf("expected 0 bytes read, got %d", n)
		}
	})

	t.Run("Negative seek", func(t *testing.T) {
		data := []byte("Hello")
		mockRA := &mockReaderAt{data: data}
		reader := Reader(mockRA, 0, int64(len(data)))

		offset, err := reader.Seek(-1, io.SeekStart)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != -1 {
			t.Fatalf("expected offset -1, got %d", offset)
		}
	})

	t.Run("Large offset", func(t *testing.T) {
		data := []byte("Hello")
		mockRA := &mockReaderAt{data: data}
		reader := Reader(mockRA, 1000, int64(len(data)))

		buf := make([]byte, 5)

		n, err := reader.Read(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}

		if n != 0 {
			t.Fatalf("expected 0 bytes read, got %d", n)
		}
	})
}
