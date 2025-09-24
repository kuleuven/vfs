package buffered

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// Mock implementations for testing
type mockReaderAt struct {
	data   []byte
	closed bool
}

func (m *mockReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if m.closed {
		return 0, errors.New("reader closed")
	}

	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}

	n := copy(p, m.data[off:])
	if off+int64(n) >= int64(len(m.data)) {
		return n, io.EOF
	}

	return n, nil
}

func (m *mockReaderAt) Close() error {
	m.closed = true
	return nil
}

type mockWriterAt struct {
	data   []byte
	closed bool
}

func (m *mockWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if m.closed {
		return 0, errors.New("writer closed")
	}

	if off+int64(len(p)) > int64(len(m.data)) {
		// Extend data if needed
		newData := make([]byte, off+int64(len(p)))
		copy(newData, m.data)
		m.data = newData
	}

	return copy(m.data[off:], p), nil
}

func (m *mockWriterAt) Close() error {
	m.closed = true
	return nil
}

type mockErrorWriter struct{}

func (m *mockErrorWriter) WriteAt(p []byte, off int64) (int, error) {
	return 0, errors.New("write error")
}

// Test Chunk functionality
func TestChunk_WriteAt(t *testing.T) {
	chunk := newChunk(10, 20)

	// Test successful write
	n, err := chunk.WriteAt([]byte("hello"), 15)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	if n != 5 {
		t.Errorf("Expected 5 bytes written, got %d", n)
	}

	if chunk.Written() != 5 {
		t.Errorf("Expected 5 total written, got %d", chunk.Written())
	}

	// Test offset too low
	_, err = chunk.WriteAt([]byte("test"), 5)
	if err == nil {
		t.Error("Expected error for offset too low")
	}

	// Test offset too high
	_, err = chunk.WriteAt([]byte("test"), 35)
	if err == nil {
		t.Error("Expected error for offset too high")
	}

	// Test overlapping writes
	n, err = chunk.WriteAt([]byte("world"), 18)
	if err != nil {
		t.Fatalf("Overlapping WriteAt failed: %v", err)
	}

	if n != 5 {
		t.Errorf("Expected 5 bytes written, got %d", n)
	}
}

func TestChunk_ReadAt(t *testing.T) {
	chunk := newChunk(10, 20)

	// Write some data first
	chunk.WriteAt([]byte("turtle"), 15)

	// Test successful read
	buf := make([]byte, 6)

	n, err := chunk.ReadAt(buf, 15)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if n != 6 {
		t.Errorf("Expected 6 bytes read, got %d", n)
	}

	if string(buf) != "turtle" {
		t.Errorf("Expected 'turtle', got '%s'", string(buf))
	}

	// Test reading from empty area
	buf = make([]byte, 5)

	_, err = chunk.ReadAt(buf, 25)
	if err != ErrNoDataAtOffset {
		t.Errorf("Expected ErrNoDataAtOffset, got %v", err)
	}

	// Test offset too low
	_, err = chunk.ReadAt(buf, 5)
	if err == nil {
		t.Error("Expected error for offset too low")
	}

	// Test offset too high
	_, err = chunk.ReadAt(buf, 35)
	if err == nil {
		t.Error("Expected error for offset too high")
	}
}

func TestChunk_Contains(t *testing.T) {
	chunk := newChunk(10, 20)

	tests := []struct {
		offset   int64
		expected bool
	}{
		{5, false},  // before chunk
		{10, true},  // start of chunk
		{20, true},  // middle of chunk
		{29, true},  // end of chunk
		{30, false}, // after chunk
	}

	for _, test := range tests {
		result := chunk.Contains(test.offset)
		if result != test.expected {
			t.Errorf("Contains(%d) = %v, expected %v", test.offset, result, test.expected)
		}
	}
}

func TestChunk_Overlaps(t *testing.T) {
	chunk := newChunk(10, 20) // covers [10, 29]

	tests := []struct {
		offset   int64
		length   int
		expected bool
	}{
		{5, 3, false},  // [5, 7] - before chunk
		{5, 8, true},   // [5, 12] - overlaps start
		{15, 5, true},  // [15, 19] - inside chunk
		{25, 10, true}, // [25, 34] - overlaps end
		{35, 5, false}, // [35, 39] - after chunk
	}

	for _, test := range tests {
		result := chunk.Overlaps(test.offset, test.length)
		if result != test.expected {
			t.Errorf("Overlaps(%d, %d) = %v, expected %v", test.offset, test.length, result, test.expected)
		}
	}
}

func TestChunk_FromReader(t *testing.T) {
	data := []byte("hello world test data")
	reader := &mockReaderAt{data: data}

	chunk := newChunk(0, 10)

	n, err := chunk.FromReader(reader)
	if err != nil {
		t.Fatalf("FromReader failed: %v", err)
	}

	if n != 10 {
		t.Errorf("Expected 10 bytes read, got %d", n)
	}

	// Test reading from chunk
	buf := make([]byte, 10)

	_, err = chunk.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt after FromReader failed: %v", err)
	}

	if string(buf) != "hello worl" {
		t.Errorf("Expected 'hello worl', got '%s'", string(buf))
	}
}

func TestChunk_WriteTo(t *testing.T) {
	chunk := newChunk(5, 10)
	chunk.WriteAt([]byte("earth"), 7)

	writer := &mockWriterAt{data: make([]byte, 20)}

	n, err := chunk.WriteTo(writer)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	if n != 5 {
		t.Errorf("Expected 5 bytes written, got %d", n)
	}

	expected := "earth"
	actual := string(writer.data[7:12])

	if actual != expected {
		t.Errorf("Expected '%s', got '%s'", expected, actual)
	}
}

func TestChunk_Reset(t *testing.T) {
	chunk := newChunk(10, 20)
	chunk.WriteAt([]byte("hello"), 15)

	chunk.Reset(30)

	if chunk.offset != 30 {
		t.Errorf("Expected offset 30, got %d", chunk.offset)
	}

	if len(chunk.slices) != 0 {
		t.Errorf("Expected empty slices, got %d", len(chunk.slices))
	}

	if chunk.Written() != 0 {
		t.Errorf("Expected 0 written bytes, got %d", chunk.Written())
	}
}

func TestChunk_IsFull(t *testing.T) {
	chunk := newChunk(0, 5)

	if chunk.IsFull() {
		t.Error("Empty chunk should not be full")
	}

	chunk.WriteAt([]byte("hello"), 0)

	if !chunk.IsFull() {
		t.Error("Fully written chunk should be full")
	}

	chunk.Reset(0)
	chunk.WriteAt([]byte("hi"), 0)

	if chunk.IsFull() {
		t.Error("Partially written chunk should not be full")
	}
}

// Test BufferedReaderAt functionality
func TestBufferedReaderAt_ReadAt(t *testing.T) {
	data := []byte("test data for buffered reading")
	reader := &mockReaderAt{data: data}

	buffered := &BufferedReaderAt{
		ReaderAt:  reader,
		ChunkSize: 10,
		MaxChunks: 2,
	}

	// Test reading first chunk
	buf := make([]byte, 4)

	n, err := buffered.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if n != 4 || string(buf) != "test" {
		t.Errorf("Expected 'test', got '%s' (%d bytes)", string(buf), n)
	}

	// Test reading across chunk boundary
	buf = make([]byte, 15)

	n, err = buffered.ReadAt(buf, 5)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt across boundary failed: %v", err)
	}

	if n < 15 {
		buf = buf[:n]
	}

	expected := "data for buffer"
	if string(buf) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(buf))
	}

	// Verify chunks are cached
	if len(buffered.chunks) == 0 {
		t.Error("Expected chunks to be cached")
	}
}

func TestBufferedReaderAt_Invalidate(t *testing.T) {
	data := []byte("hello world test")
	reader := &mockReaderAt{data: data}

	buffered := &BufferedReaderAt{
		ReaderAt:  reader,
		ChunkSize: 8,
		MaxChunks: 2,
	}

	// Read to populate chunks
	buf := make([]byte, 16)
	buffered.ReadAt(buf, 0)

	initialChunks := len(buffered.chunks)
	if initialChunks == 0 {
		t.Fatal("Expected chunks to be populated")
	}

	// Invalidate overlapping range
	buffered.Invalidate(5, 10)

	// Should have fewer chunks now
	if len(buffered.chunks) >= initialChunks {
		t.Error("Expected chunks to be invalidated")
	}
}

func TestBufferedReaderAt_Close(t *testing.T) {
	reader := &mockReaderAt{data: []byte("test")}
	buffered := &BufferedReaderAt{ReaderAt: reader}

	err := buffered.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !reader.closed {
		t.Error("Expected underlying reader to be closed")
	}
}

// Test BufferedWriterAt functionality
func TestBufferedWriterAt_WriteAt(t *testing.T) {
	writer := &mockWriterAt{data: make([]byte, 100)}

	buffered := &BufferedWriterAt{
		WriterAt:  writer,
		ChunkSize: 10,
		MaxChunks: 2,
	}

	// Test writing
	data := []byte("hello")

	n, err := buffered.WriteAt(data, 5)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Expected %d bytes written, got %d", len(data), n)
	}

	// Verify chunk is cached
	if len(buffered.chunks) == 0 {
		t.Error("Expected chunk to be cached")
	}
}

func TestBufferedWriterAt_Close(t *testing.T) {
	writer := &mockWriterAt{data: make([]byte, 100)}

	buffered := &BufferedWriterAt{
		WriterAt:  writer,
		ChunkSize: 10,
		MaxChunks: 1,
	}

	// Write some data
	buffered.WriteAt([]byte("butterfly"), 5)

	err := buffered.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !writer.closed {
		t.Error("Expected underlying writer to be closed")
	}

	// Verify data was flushed
	expected := "butterfly"
	actual := string(writer.data[5:14])

	if actual != expected {
		t.Errorf("Expected '%s', got '%s'", expected, actual)
	}
}

func TestBufferedWriterAt_ChunkEviction(t *testing.T) {
	writer := &mockWriterAt{data: make([]byte, 100)}

	buffered := &BufferedWriterAt{
		WriterAt:  writer,
		ChunkSize: 10,
		MaxChunks: 1,
	}

	// Write to first chunk
	buffered.WriteAt([]byte("first"), 0)

	if len(buffered.chunks) != 1 {
		t.Fatalf("Expected 1 chunk, got %d", len(buffered.chunks))
	}

	// Write to second chunk location - should evict first
	buffered.WriteAt([]byte("second"), 20)

	if len(buffered.chunks) != 1 {
		t.Errorf("Expected 1 chunk after eviction, got %d", len(buffered.chunks))
	}

	// First chunk should have been written to underlying writer
	expected := "first"
	actual := string(writer.data[0:5])

	if actual != expected {
		t.Errorf("Expected '%s' in writer, got '%s'", expected, actual)
	}
}

// Test BackgroundReader functionality
func TestBackgroundReader_ReadAt(t *testing.T) {
	data := []byte("hello world background test")
	reader := &mockReaderAt{data: data}

	bgReader := &BackgroundReader{
		ReaderAt:  reader,
		ChunkSize: 10,
	}

	buf := make([]byte, 5)

	n, err := bgReader.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if n != 5 || string(buf) != "hello" {
		t.Errorf("Expected 'hello', got '%s' (%d bytes)", string(buf), n)
	}

	// Wait for background operation
	bgReader.Wait()

	// Second read should use cached chunk
	buf2 := make([]byte, 5)

	_, err = bgReader.ReadAt(buf2, 6)
	if err != nil {
		t.Fatalf("Second ReadAt failed: %v", err)
	}

	if string(buf2) != "world" {
		t.Errorf("Expected 'world', got '%s'", string(buf2))
	}
}

func TestBackgroundReader_Close(t *testing.T) {
	reader := &mockReaderAt{data: []byte("test")}
	bgReader := &BackgroundReader{ReaderAt: reader}

	err := bgReader.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !reader.closed {
		t.Error("Expected underlying reader to be closed")
	}
}

// Test BackgroundWriter functionality
func TestBackgroundWriter_WriteAt(t *testing.T) {
	writer := &mockWriterAt{data: make([]byte, 100)}

	bgWriter := &BackgroundWriter{
		WriterAt:  writer,
		ChunkSize: 10,
	}

	data := []byte("hello")

	n, err := bgWriter.WriteAt(data, 5)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Expected %d bytes written, got %d", len(data), n)
	}

	// Wait for background operation
	bgWriter.Wait()

	// Verify data was written
	expected := "hello"
	actual := string(writer.data[5:10])

	if actual != expected {
		t.Errorf("Expected '%s', got '%s'", expected, actual)
	}
}

func TestBackgroundWriter_Close(t *testing.T) {
	writer := &mockWriterAt{data: make([]byte, 100)}
	bgWriter := &BackgroundWriter{WriterAt: writer}

	err := bgWriter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !writer.closed {
		t.Error("Expected underlying writer to be closed")
	}
}

func TestBackgroundWriter_Error(t *testing.T) {
	errorWriter := &mockErrorWriter{}
	bgWriter := &BackgroundWriter{
		WriterAt:  errorWriter,
		ChunkSize: 10,
	}

	// First write should succeed (buffered)
	_, err := bgWriter.WriteAt([]byte("test"), 0)
	if err != nil {
		t.Fatalf("First WriteAt should succeed: %v", err)
	}

	// Wait for background operation to complete with error
	bgWriter.Wait()

	// Second write should return the background error
	_, err = bgWriter.WriteAt([]byte("test2"), 10)
	if err == nil {
		t.Error("Expected error from background operation")
	}
}

// Test error handling
func TestReturnEOF(t *testing.T) {
	n, err := returnEOF(5, ErrNoDataAtOffset)
	if err != io.EOF {
		t.Errorf("Expected io.EOF, got %v", err)
	}

	if n != 5 {
		t.Errorf("Expected n=5, got %d", n)
	}

	n, err = returnEOF(3, errors.New("other error"))
	if err.Error() != "other error" {
		t.Errorf("Expected 'other error', got %v", err)
	}

	if n != 3 {
		t.Errorf("Expected n=3, got %d", n)
	}
}

// Test MultipleError functionality
func TestMultipleError(t *testing.T) {
	// Test no errors
	err := MultipleError()
	if err != nil {
		t.Errorf("Expected nil, got %v", err)
	}

	// Test single error
	singleErr := errors.New("single error")

	err = MultipleError(singleErr)
	if err != singleErr {
		t.Errorf("Expected %v, got %v", singleErr, err)
	}

	// Test multiple errors
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	err = MultipleError(err1, err2)

	multiErr, ok := err.(MultipleErrors)
	if !ok {
		t.Fatalf("Expected MultipleErrors, got %T", err)
	}

	if len(multiErr) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(multiErr))
	}

	errStr := multiErr.Error()
	if !strings.Contains(errStr, "error 1") || !strings.Contains(errStr, "error 2") {
		t.Errorf("Error string should contain both errors: %s", errStr)
	}
}

// Test concurrent access
func TestConcurrentAccess(t *testing.T) {
	data := bytes.Repeat([]byte("test data "), 100)
	reader := &mockReaderAt{data: data}

	buffered := &BufferedReaderAt{
		ReaderAt:  reader,
		ChunkSize: 50,
		MaxChunks: 5,
	}

	var wg sync.WaitGroup

	errorch := make(chan error, 10)

	// Start multiple goroutines reading concurrently
	for i := range 10 {
		wg.Add(1)

		go func(offset int) {
			defer wg.Done()

			buf := make([]byte, 20)

			_, err := buffered.ReadAt(buf, int64(offset*10))
			if err != nil && err != io.EOF {
				errorch <- err
			}
		}(i)
	}

	wg.Wait()
	close(errorch)

	// Check for any errors
	for err := range errorch {
		t.Errorf("Concurrent read error: %v", err)
	}
}

func TestEdgeCases(t *testing.T) {
	// Test empty reader
	emptyReader := &mockReaderAt{data: []byte{}}
	buffered := &BufferedReaderAt{
		ReaderAt:  emptyReader,
		ChunkSize: 10,
		MaxChunks: 1,
	}

	buf := make([]byte, 5)

	n, err := buffered.ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("Expected EOF for empty reader, got %v", err)
	}

	if n != 0 {
		t.Errorf("Expected 0 bytes read, got %d", n)
	}

	// Test zero-sized chunk
	chunk := newChunk(0, 0)

	_, err = chunk.WriteAt([]byte("test"), 0)
	if err == nil {
		t.Error("Expected error for zero-sized chunk write")
	}
}
