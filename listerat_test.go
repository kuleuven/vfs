package vfs

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// Mock FileInfo implementation for testing
type mockFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) Size() int64        { return m.size }
func (m mockFileInfo) Mode() os.FileMode  { return m.mode }
func (m mockFileInfo) ModTime() time.Time { return m.modTime }
func (m mockFileInfo) IsDir() bool        { return m.isDir }
func (m mockFileInfo) Sys() interface{}   { return nil }
func (m mockFileInfo) Uid() uint32        { return 1000 } //nolint:staticcheck
func (m mockFileInfo) Gid() uint32        { return 1000 } //nolint:staticcheck
func (m mockFileInfo) NumLinks() uint64   { return 1 }

func (m mockFileInfo) Extended() (Attributes, error) {
	return Attributes{}, nil
}

func (m mockFileInfo) Permissions() (*Permissions, error) {
	return &Permissions{Read: true}, nil
}

func newMockFileInfo(name string) FileInfo {
	return mockFileInfo{
		name:    name,
		size:    100,
		mode:    0o644,
		modTime: time.Now(),
		isDir:   false,
	}
}

func TestFileInfoListerAt_ListAt(t *testing.T) { //nolint:funlen
	files := FileInfoListerAt{
		newMockFileInfo("file1.txt"),
		newMockFileInfo("file2.txt"),
		newMockFileInfo("file3.txt"),
		newMockFileInfo("file4.txt"),
		newMockFileInfo("file5.txt"),
	}

	tests := []struct {
		name      string
		bufSize   int
		offset    int64
		wantN     int
		wantErr   error
		wantNames []string
	}{
		{
			name:      "read all from start",
			bufSize:   10,
			offset:    0,
			wantN:     5,
			wantErr:   io.EOF,
			wantNames: []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"},
		},
		{
			name:      "read partial",
			bufSize:   3,
			offset:    0,
			wantN:     3,
			wantErr:   nil,
			wantNames: []string{"file1.txt", "file2.txt", "file3.txt"},
		},
		{
			name:      "read from middle",
			bufSize:   3,
			offset:    2,
			wantN:     3,
			wantErr:   io.EOF,
			wantNames: []string{"file3.txt", "file4.txt", "file5.txt"},
		},
		{
			name:      "read beyond end",
			bufSize:   3,
			offset:    5,
			wantN:     0,
			wantErr:   io.EOF,
			wantNames: []string{},
		},
		{
			name:      "read last item",
			bufSize:   5,
			offset:    4,
			wantN:     1,
			wantErr:   io.EOF,
			wantNames: []string{"file5.txt"},
		},
		{
			name:      "buffer larger than remaining",
			bufSize:   10,
			offset:    3,
			wantN:     2,
			wantErr:   io.EOF,
			wantNames: []string{"file4.txt", "file5.txt"},
		},
		{
			name:      "offset at end",
			bufSize:   1,
			offset:    5,
			wantN:     0,
			wantErr:   io.EOF,
			wantNames: []string{},
		},
		{
			name:      "read one at a time",
			bufSize:   1,
			offset:    0,
			wantN:     1,
			wantErr:   nil,
			wantNames: []string{"file1.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]FileInfo, tt.bufSize)
			n, err := files.ListAt(buf, tt.offset)

			if n != tt.wantN {
				t.Errorf("ListAt() n = %d, want %d", n, tt.wantN)
			}

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ListAt() error = %v, want %v", err, tt.wantErr)
			}

			if n > 0 {
				for i := 0; i < n && i < len(tt.wantNames); i++ {
					if buf[i].Name() != tt.wantNames[i] {
						t.Errorf("ListAt() buf[%d].Name() = %q, want %q", i, buf[i].Name(), tt.wantNames[i])
					}
				}
			}
		})
	}
}

func TestFileInfoListerAt_ListAt_EmptyList(t *testing.T) {
	files := FileInfoListerAt{}
	buf := make([]FileInfo, 10)

	n, err := files.ListAt(buf, 0)

	if n != 0 {
		t.Errorf("ListAt() on empty list n = %d, want 0", n)
	}

	if !errors.Is(err, io.EOF) {
		t.Errorf("ListAt() on empty list error = %v, want io.EOF", err)
	}
}

func TestFileInfoListerAt_Close(t *testing.T) {
	files := FileInfoListerAt{
		newMockFileInfo("file1.txt"),
	}

	err := files.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestListAll(t *testing.T) { //nolint:gocognit,funlen
	tests := []struct {
		name      string
		lister    ListerAt
		wantCount int
		wantErr   bool
		wantNames []string
	}{
		{
			name: "list all files",
			lister: FileInfoListerAt{
				newMockFileInfo("file1.txt"),
				newMockFileInfo("file2.txt"),
				newMockFileInfo("file3.txt"),
			},
			wantCount: 3,
			wantErr:   false,
			wantNames: []string{"file1.txt", "file2.txt", "file3.txt"},
		},
		{
			name:      "empty list",
			lister:    FileInfoListerAt{},
			wantCount: 0,
			wantErr:   false,
			wantNames: []string{},
		},
		{
			name: "large list",
			lister: FileInfoListerAt{
				newMockFileInfo("file1.txt"),
				newMockFileInfo("file2.txt"),
				newMockFileInfo("file3.txt"),
				newMockFileInfo("file4.txt"),
				newMockFileInfo("file5.txt"),
				newMockFileInfo("file6.txt"),
				newMockFileInfo("file7.txt"),
				newMockFileInfo("file8.txt"),
				newMockFileInfo("file9.txt"),
				newMockFileInfo("file10.txt"),
			},
			wantCount: 10,
			wantErr:   false,
		},
		{
			name:      "error from lister",
			lister:    &errorListerAt{errAt: 0},
			wantCount: 0,
			wantErr:   true,
		},
		{
			name:      "error after some items",
			lister:    &errorListerAt{errAt: 50},
			wantCount: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ListAll(tt.lister)

			if (err != nil) != tt.wantErr {
				t.Errorf("ListAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if len(result) != tt.wantCount {
					t.Errorf("ListAll() returned %d items, want %d", len(result), tt.wantCount)
				}

				if tt.wantNames != nil {
					for i, name := range tt.wantNames {
						if i < len(result) && result[i].Name() != name {
							t.Errorf("ListAll() result[%d].Name() = %q, want %q", i, result[i].Name(), name)
						}
					}
				}
			}
		})
	}
}

func TestIterate(t *testing.T) { //nolint:gocognit,funlen
	tests := []struct {
		name      string
		lister    ListerAt
		batchSize int
		wantNames []string
		stopAfter int // Stop iteration after N items (0 = iterate all)
	}{
		{
			name: "iterate all files",
			lister: FileInfoListerAt{
				newMockFileInfo("file1.txt"),
				newMockFileInfo("file2.txt"),
				newMockFileInfo("file3.txt"),
			},
			batchSize: 2,
			wantNames: []string{"file1.txt", "file2.txt", "file3.txt"},
			stopAfter: 0,
		},
		{
			name:      "iterate empty list",
			lister:    FileInfoListerAt{},
			batchSize: 10,
			wantNames: []string{},
			stopAfter: 0,
		},
		{
			name: "iterate with batch size 1",
			lister: FileInfoListerAt{
				newMockFileInfo("file1.txt"),
				newMockFileInfo("file2.txt"),
			},
			batchSize: 1,
			wantNames: []string{"file1.txt", "file2.txt"},
			stopAfter: 0,
		},
		{
			name: "early termination",
			lister: FileInfoListerAt{
				newMockFileInfo("file1.txt"),
				newMockFileInfo("file2.txt"),
				newMockFileInfo("file3.txt"),
				newMockFileInfo("file4.txt"),
			},
			batchSize: 2,
			wantNames: []string{"file1.txt", "file2.txt"},
			stopAfter: 2,
		},
		{
			name: "large batch size",
			lister: FileInfoListerAt{
				newMockFileInfo("file1.txt"),
				newMockFileInfo("file2.txt"),
			},
			batchSize: 100,
			wantNames: []string{"file1.txt", "file2.txt"},
			stopAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotNames []string

			count := 0

			for entry := range Iterate(tt.lister, tt.batchSize) {
				gotNames = append(gotNames, entry.Name())

				count++
				if tt.stopAfter > 0 && count >= tt.stopAfter {
					break
				}
			}

			expectedCount := len(tt.wantNames)
			if tt.stopAfter > 0 && tt.stopAfter < len(tt.wantNames) {
				expectedCount = tt.stopAfter
			}

			if len(gotNames) != expectedCount {
				t.Errorf("Iterate() got %d items, want %d", len(gotNames), expectedCount)
			}

			for i, name := range gotNames {
				if i < len(tt.wantNames) && name != tt.wantNames[i] {
					t.Errorf("Iterate() item[%d] = %q, want %q", i, name, tt.wantNames[i])
				}
			}
		})
	}
}

func TestIterate_WithError(t *testing.T) {
	lister := &errorListerAt{errAt: 50}

	var count int
	for range Iterate(lister, 10) {
		count++
	}

	// Should stop on error (before reaching 100 items)
	if count >= 100 {
		t.Errorf("Iterate() with error returned %d items, expected to stop early", count)
	}
}

// Mock ListerAt that returns an error
type errorListerAt struct {
	errAt int
}

func (e *errorListerAt) ListAt(buf []FileInfo, offset int64) (int, error) {
	if offset >= int64(e.errAt) {
		return 0, errors.New("mock error")
	}

	// Return some mock data
	n := min(len(buf), e.errAt-int(offset))

	for i := range n {
		buf[i] = newMockFileInfo("file.txt")
	}

	if int(offset)+n >= e.errAt {
		return n, errors.New("mock error")
	}

	return n, nil
}

func (e *errorListerAt) Close() error {
	return nil
}
