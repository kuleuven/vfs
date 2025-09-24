//nolint:forcetypeassert,goconst
package writerat

import (
	"errors"
	"io"
	"os"
	"sync"
	"testing"
)

// Mock WriterAt for testing
type mockWriterAt struct {
	data []byte
	mu   sync.Mutex
}

func newMockWriterAt(size int) *mockWriterAt {
	return &mockWriterAt{
		data: make([]byte, size),
	}
}

func (m *mockWriterAt) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if off < 0 {
		return 0, errors.New("negative offset")
	}

	// Expand data if necessary
	if off+int64(len(p)) > int64(len(m.data)) {
		newSize := off + int64(len(p))
		newData := make([]byte, newSize)
		copy(newData, m.data)
		m.data = newData
	}

	n := copy(m.data[off:], p)

	return n, nil
}

func (m *mockWriterAt) getData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]byte, len(m.data))
	copy(result, m.data)

	return result
}

// Mock WriteSeeker for testing
type mockWriteSeeker struct {
	data   []byte
	offset int64
	mu     sync.Mutex
}

func newMockWriteSeeker() *mockWriteSeeker {
	return &mockWriteSeeker{
		data: make([]byte, 0),
	}
}

func (m *mockWriteSeeker) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.offset < 0 {
		return 0, errors.New("negative offset")
	}

	// Expand data if necessary
	if m.offset+int64(len(p)) > int64(len(m.data)) {
		newSize := m.offset + int64(len(p))
		newData := make([]byte, newSize)
		copy(newData, m.data)
		m.data = newData
	}

	n := copy(m.data[m.offset:], p)
	m.offset += int64(n)

	return n, nil
}

func (m *mockWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	}

	return m.offset, nil
}

func (m *mockWriteSeeker) getData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]byte, len(m.data))
	copy(result, m.data)

	return result
}

func TestWriter(t *testing.T) { //nolint:gocognit,funlen
	t.Run("Basic writing", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 0, 0)

		data := []byte("Hello")

		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != len(data) {
			t.Fatalf("expected %d bytes written, got %d", len(data), n)
		}

		// Check offset
		if writer.Offset() != int64(len(data)) {
			t.Fatalf("expected offset %d, got %d", len(data), writer.Offset())
		}

		// Check written data
		written := mockWA.getData()
		if string(written[:len(data)]) != "Hello" {
			t.Fatalf("expected 'Hello', got '%s'", string(written[:len(data)]))
		}
	})

	t.Run("Writing with initial offset", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 10, 10)

		data := []byte("World")

		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != len(data) {
			t.Fatalf("expected %d bytes written, got %d", len(data), n)
		}

		// Check offset
		expectedOffset := int64(10 + len(data))
		if writer.Offset() != expectedOffset {
			t.Fatalf("expected offset %d, got %d", expectedOffset, writer.Offset())
		}

		// Check written data at correct position
		written := mockWA.getData()
		if string(written[10:15]) != "World" {
			t.Fatalf("expected 'World', got '%s'", string(written[10:15]))
		}
	})

	t.Run("Multiple writes", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 0, 0)

		// First write
		data1 := []byte("Hello")

		n1, err := writer.Write(data1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Second write
		data2 := []byte(", World!")

		n2, err := writer.Write(data2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n1 != len(data1) || n2 != len(data2) {
			t.Fatalf("unexpected write lengths: %d, %d", n1, n2)
		}

		// Check final content
		written := mockWA.getData()
		expected := "Hello, World!"

		if string(written[:len(expected)]) != expected {
			t.Fatalf("expected '%s', got '%s'", expected, string(written[:len(expected)]))
		}
	})

	t.Run("Length tracking", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 0, 5) // Initial length 5

		// Write beyond initial length
		data := []byte("Hello, World!")
		writer.Write(data)

		// Length should be updated
		if writer.(*writerFromWriterAt).length != int64(len(data)) {
			t.Fatalf("expected length %d, got %d", len(data), writer.(*writerFromWriterAt).length)
		}
	})

	t.Run("Zero-length write", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 0, 0)

		data := []byte{}

		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != 0 {
			t.Fatalf("expected 0 bytes written, got %d", n)
		}

		if writer.Offset() != 0 {
			t.Fatalf("expected offset 0, got %d", writer.Offset())
		}
	})
}

func TestWriterSeek(t *testing.T) { //nolint:gocognit,funlen
	mockWA := newMockWriterAt(100)

	t.Run("Seek from start", func(t *testing.T) {
		writer := Writer(mockWA, 0, 20)

		offset, err := writer.Seek(10, io.SeekStart)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != 10 {
			t.Fatalf("expected offset 10, got %d", offset)
		}

		data := []byte("Test")
		writer.Write(data)

		written := mockWA.getData()
		if string(written[10:14]) != "Test" {
			t.Fatalf("expected 'Test', got '%s'", string(written[10:14]))
		}
	})

	t.Run("Seek from current", func(t *testing.T) {
		writer := Writer(mockWA, 5, 20)

		// First write to advance position
		writer.Write([]byte("AB"))

		// Seek relative to current position
		offset, err := writer.Seek(3, io.SeekCurrent)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != 10 {
			t.Fatalf("expected offset 10, got %d", offset)
		}

		writer.Write([]byte("Test"))

		written := mockWA.getData()
		if string(written[10:14]) != "Test" {
			t.Fatalf("expected 'Test', got '%s'", string(written[10:14]))
		}
	})

	t.Run("Seek from end", func(t *testing.T) {
		writer := Writer(mockWA, 0, 20)

		offset, err := writer.Seek(-5, io.SeekEnd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != 15 {
			t.Fatalf("expected offset 15, got %d", offset)
		}

		writer.Write([]byte("End"))

		written := mockWA.getData()
		if string(written[15:18]) != "End" {
			t.Fatalf("expected 'End', got '%s'", string(written[15:18]))
		}
	})

	t.Run("Seek from end with unknown length", func(t *testing.T) {
		writer := Writer(mockWA, 0, -1) // Unknown length

		_, err := writer.Seek(-5, io.SeekEnd)
		if !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("expected ErrInvalid, got %v", err)
		}
	})

	t.Run("Negative seek positions", func(t *testing.T) {
		writer := Writer(mockWA, 0, 20)

		// Seek to negative position should work
		offset, err := writer.Seek(-5, io.SeekStart)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != -5 {
			t.Fatalf("expected offset -5, got %d", offset)
		}
	})
}

func TestWriterAt(t *testing.T) { //nolint:gocognit,funlen
	t.Run("Basic WriteAt functionality", func(t *testing.T) {
		mockWS := newMockWriteSeeker()
		writerAt := WriterAt(mockWS)

		data := []byte("Hello")

		n, err := writerAt.WriteAt(data, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != len(data) {
			t.Fatalf("expected %d bytes written, got %d", len(data), n)
		}

		written := mockWS.getData()
		if string(written[:len(data)]) != "Hello" {
			t.Fatalf("expected 'Hello', got '%s'", string(written[:len(data)]))
		}
	})

	t.Run("WriteAt at different positions", func(t *testing.T) {
		mockWS := newMockWriteSeeker()
		writerAt := WriterAt(mockWS)

		// Write at position 0
		data1 := []byte("Hello")

		n1, err := writerAt.WriteAt(data1, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Write at position 10
		data2 := []byte("World")

		n2, err := writerAt.WriteAt(data2, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n1 != len(data1) || n2 != len(data2) {
			t.Fatalf("unexpected write lengths: %d, %d", n1, n2)
		}

		written := mockWS.getData()
		if string(written[:5]) != "Hello" {
			t.Fatalf("expected 'Hello' at start, got '%s'", string(written[:5]))
		}

		if string(written[10:15]) != "World" {
			t.Fatalf("expected 'World' at position 10, got '%s'", string(written[10:15]))
		}
	})

	t.Run("Sequential WriteAt calls", func(t *testing.T) {
		mockWS := newMockWriteSeeker()
		writerAt := WriterAt(mockWS)

		// Multiple writes at same position should overwrite
		writerAt.WriteAt([]byte("First"), 5)
		writerAt.WriteAt([]byte("Second"), 5)

		written := mockWS.getData()
		if string(written[5:11]) != "Second" {
			t.Fatalf("expected 'Second', got '%s'", string(written[5:11]))
		}
	})

	t.Run("WriteAt with seek optimization", func(t *testing.T) {
		mockWS := newMockWriteSeeker()
		writerAt := WriterAt(mockWS)

		// First write
		writerAt.WriteAt([]byte("Hello"), 0)

		// Second write at consecutive position (should not require seek)
		writerAt.WriteAt([]byte("World"), 5)

		written := mockWS.getData()
		expected := "HelloWorld"

		if string(written[:len(expected)]) != expected {
			t.Fatalf("expected '%s', got '%s'", expected, string(written[:len(expected)]))
		}
	})

	t.Run("WriteAt expanding data", func(t *testing.T) {
		mockWS := newMockWriteSeeker()
		writerAt := WriterAt(mockWS)

		// Write at a large offset
		data := []byte("Far")
		writerAt.WriteAt(data, 100)

		written := mockWS.getData()
		if len(written) < 103 {
			t.Fatalf("expected data to expand to at least 103 bytes, got %d", len(written))
		}

		if string(written[100:103]) != "Far" {
			t.Fatalf("expected 'Far' at position 100, got '%s'", string(written[100:103]))
		}
	})
}

func TestConcurrentAccess(t *testing.T) { //nolint:gocognit,funlen
	t.Run("Concurrent writes to OffsetWriter", func(t *testing.T) {
		mockWA := newMockWriterAt(10000)
		writer := Writer(mockWA, 0, 0)

		const numGoroutines = 10

		const numWrites = 100

		var wg sync.WaitGroup

		errorch := make(chan error, numGoroutines)

		for range numGoroutines {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range numWrites {
					data := []byte("X")

					_, err := writer.Write(data)
					if err != nil {
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
			t.Fatalf("concurrent write error: %v", err)
		}

		// Verify total writes
		finalOffset := writer.Offset()
		expectedWrites := int64(numGoroutines * numWrites)

		if finalOffset != expectedWrites {
			t.Fatalf("expected offset %d, got %d", expectedWrites, finalOffset)
		}

		// Verify all writes were 'X'
		written := mockWA.getData()
		for i := range finalOffset {
			if written[i] != 'X' {
				t.Fatalf("expected 'X' at position %d, got '%c'", i, written[i])
			}
		}
	})

	t.Run("Concurrent seeks and writes", func(t *testing.T) {
		mockWA := newMockWriterAt(1000)
		writer := Writer(mockWA, 0, 100)

		const numGoroutines = 10

		var wg sync.WaitGroup

		errorch := make(chan error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				// Seek to different positions
				offset := int64(id * 10)

				_, err := writer.Seek(offset, io.SeekStart)
				if err != nil {
					errorch <- err
					return
				}

				// Write after seek
				data := []byte{byte('A' + id)}

				_, err = writer.Write(data)
				if err != nil {
					errorch <- err
					return
				}
			}(i)
		}

		wg.Wait()
		close(errorch)

		// Check for any errors
		for err := range errorch {
			t.Fatalf("concurrent seek/write error: %v", err)
		}

		// Verify writes landed in expected positions
		written := mockWA.getData()

		for i := range numGoroutines {
			pos := i * 10
			if pos < len(written) && written[pos] != 0 {
				// Should be one of the expected characters
				expected := byte('A' + i)
				if written[pos] != expected {
					// Due to concurrency, the exact character may vary
					// but it should be within the valid range
					if written[pos] < 'A' || written[pos] > 'A'+byte(numGoroutines-1) {
						t.Fatalf("unexpected character at position %d: %c", pos, written[pos])
					}
				}
			}
		}
	})

	t.Run("Concurrent WriteAt calls", func(t *testing.T) {
		mockWS := newMockWriteSeeker()
		writerAt := WriterAt(mockWS)

		const numGoroutines = 10

		const numWrites = 100

		var wg sync.WaitGroup

		errorch := make(chan error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				for j := range numWrites {
					offset := int64(id*numWrites + j)
					data := []byte{byte('A' + (id % 26))}

					_, err := writerAt.WriteAt(data, offset)
					if err != nil {
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
			t.Fatalf("concurrent WriteAt error: %v", err)
		}

		// Verify data integrity
		written := mockWS.getData()
		expectedSize := numGoroutines * numWrites

		if len(written) < expectedSize {
			t.Fatalf("expected at least %d bytes written, got %d", expectedSize, len(written))
		}
	})
}

func TestEdgeCases(t *testing.T) { //nolint:gocognit,funlen
	t.Run("Write to negative offset after seek", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 0, 10)

		// Seek to negative position
		writer.Seek(-5, io.SeekStart)

		// Write should handle negative offset gracefully or error
		data := []byte("Test")

		_, err := writer.Write(data)
		if err == nil {
			t.Log("Write to negative offset succeeded (implementation-dependent)")
		} else {
			t.Log("Write to negative offset failed as expected:", err)
		}
	})

	t.Run("Large offset write", func(t *testing.T) {
		mockWA := newMockWriterAt(10)
		writer := Writer(mockWA, 1000, 1000)

		data := []byte("Test")

		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != len(data) {
			t.Fatalf("expected %d bytes written, got %d", len(data), n)
		}

		// Verify data was written at correct offset
		written := mockWA.getData()
		if len(written) < 1004 {
			t.Fatalf("expected data to expand to at least 1004 bytes")
		}

		if string(written[1000:1004]) != "Test" {
			t.Fatalf("expected 'Test' at offset 1000, got '%s'", string(written[1000:1004]))
		}
	})

	t.Run("WriteAt with negative offset", func(t *testing.T) {
		mockWS := newMockWriteSeeker()
		writerAt := WriterAt(mockWS)

		data := []byte("Test")

		_, err := writerAt.WriteAt(data, -5)
		if err != nil {
			t.Log("WriteAt with negative offset failed as expected:", err)
		}
	})

	t.Run("Empty write operations", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 0, 0)

		// Write empty data
		n, err := writer.Write([]byte{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if n != 0 {
			t.Fatalf("expected 0 bytes written, got %d", n)
		}

		if writer.Offset() != 0 {
			t.Fatalf("expected offset to remain 0, got %d", writer.Offset())
		}
	})

	t.Run("Length boundary conditions", func(t *testing.T) {
		mockWA := newMockWriterAt(100)
		writer := Writer(mockWA, 0, 5)

		// Write exactly to length boundary
		writer.Write([]byte("Hello"))

		if writer.(*writerFromWriterAt).length != 5 {
			t.Fatalf("expected length to remain 5, got %d", writer.(*writerFromWriterAt).length)
		}

		// Write beyond length boundary
		writer.Write([]byte("!"))

		if writer.(*writerFromWriterAt).length != 6 {
			t.Fatalf("expected length to update to 6, got %d", writer.(*writerFromWriterAt).length)
		}
	})
}

func TestIntegration(t *testing.T) {
	t.Run("Writer and WriterAt integration", func(t *testing.T) {
		// Mock the Seek method for bytes.Buffer
		ws := &mockWriteSeeker{}
		writerAt := WriterAt(ws)

		// Write some data using WriterAt
		writerAt.WriteAt([]byte("Hello"), 0)
		writerAt.WriteAt([]byte("World"), 6)

		written := ws.getData()
		if string(written[:5]) != "Hello" {
			t.Fatalf("expected 'Hello', got '%s'", string(written[:5]))
		}

		if string(written[6:11]) != "World" {
			t.Fatalf("expected 'World', got '%s'", string(written[6:11]))
		}
	})

	t.Run("Complex seek and write patterns", func(t *testing.T) {
		mockWA := newMockWriterAt(1000)
		writer := Writer(mockWA, 0, 0)

		// Pattern: write, seek, write, seek back, overwrite
		writer.Write([]byte("First"))
		writer.Seek(10, io.SeekStart)
		writer.Write([]byte("Second"))
		writer.Seek(0, io.SeekStart)
		writer.Write([]byte("FIRST"))

		written := mockWA.getData()
		if string(written[:5]) != "FIRST" {
			t.Fatalf("expected 'FIRST', got '%s'", string(written[:5]))
		}

		if string(written[10:16]) != "Second" {
			t.Fatalf("expected 'Second', got '%s'", string(written[10:16]))
		}
	})
}
