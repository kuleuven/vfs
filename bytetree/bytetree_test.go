package bytetree

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Check that files were created
	if _, err := os.Stat(filepath.Join(tmpDir, "inodes.db")); os.IsNotExist(err) {
		t.Error("inodes.db was not created")
	}

	if _, err := os.Stat(filepath.Join(tmpDir, "files.db")); os.IsNotExist(err) {
		t.Error("files.db was not created")
	}
}

func TestNewInvalidDirectory(t *testing.T) {
	// Try to create ByteTree in non-existent directory
	_, err := New("/non/existent/directory")
	if err == nil {
		t.Error("Expected error when creating ByteTree in non-existent directory")
	}
}

func TestPutAndGet(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	defer tree.Close()

	// Test basic put and get
	value := Value{
		Handle: []byte("test"),
		Path:   "/path/to/file",
	}

	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	path, err := tree.Get([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	if path != "/path/to/file" {
		t.Errorf("Expected path '/path/to/file', got '%s'", path)
	}
}

func TestGetNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	_, err = tree.Get([]byte("nonexistent"))
	if err != os.ErrNotExist {
		t.Errorf("Expected os.ErrNotExist, got %v", err)
	}
}

func TestPutSameHandleDifferentPath(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Put first value
	value1 := Value{
		Handle: []byte("test"),
		Path:   "/path1",
	}

	err = tree.Put(value1)
	if err != nil {
		t.Fatal(err)
	}

	// Put second value with same handle but different path
	value2 := Value{
		Handle: []byte("test"),
		Path:   "/path2",
	}

	err = tree.Put(value2)
	if err != nil {
		t.Fatal(err)
	}

	// Should get the updated path
	path, err := tree.Get([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	if path != "/path2" {
		t.Errorf("Expected path '/path2', got '%s'", path)
	}
}

func TestPutSameHandleSamePath(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	value := Value{
		Handle: []byte("test"),
		Path:   "/path",
	}

	// Put same value twice
	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	path, err := tree.Get([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	if path != "/path" {
		t.Errorf("Expected path '/path', got '%s'", path)
	}
}

func TestPutMultipleValues(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	values := []Value{
		{Handle: []byte("a"), Path: "/path/a"},
		{Handle: []byte("ab"), Path: "/path/ab"},
		{Handle: []byte("abc"), Path: "/path/abc"},
		{Handle: []byte("b"), Path: "/path/b"},
	}

	// Put all values
	for _, v := range values {
		err = tree.Put(v)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get and verify all values
	for _, v := range values {
		path, err := tree.Get(v.Handle)
		if err != nil {
			t.Fatal(err)
		}

		if path != v.Path {
			t.Errorf("Expected path '%s', got '%s' for handle %s", v.Path, path, string(v.Handle))
		}
	}
}

func TestPutPrefixConflict(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Put value with handle "test"
	value1 := Value{
		Handle: []byte("test"),
		Path:   "/path1",
	}

	err = tree.Put(value1)
	if err != nil {
		t.Fatal(err)
	}

	// Put value with handle "te" (prefix of "test")
	value2 := Value{
		Handle: []byte("te"),
		Path:   "/path2",
	}

	err = tree.Put(value2)
	if err != nil {
		t.Fatal(err)
	}

	// Both should be retrievable
	path1, err := tree.Get([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	if path1 != "/path1" {
		t.Errorf("Expected path '/path1', got '%s'", path1)
	}

	path2, err := tree.Get([]byte("te"))
	if err != nil {
		t.Fatal(err)
	}

	if path2 != "/path2" {
		t.Errorf("Expected path '/path2', got '%s'", path2)
	}
}

func TestEmptyHandle(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Test empty handle
	value := Value{
		Handle: []byte{},
		Path:   "/empty",
	}

	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	path, err := tree.Get([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	if path != "/empty" {
		t.Errorf("Expected path '/empty', got '%s'", path)
	}
}

func TestBinaryHandles(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Test with binary data including null bytes
	value := Value{
		Handle: []byte{0, 1, 2, 255, 0},
		Path:   "/binary/path",
	}

	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	path, err := tree.Get([]byte{0, 1, 2, 255, 0})
	if err != nil {
		t.Fatal(err)
	}

	if path != "/binary/path" {
		t.Errorf("Expected path '/binary/path', got '%s'", path)
	}
}

func TestGetPartialMatch(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Put value with handle "test"
	value := Value{
		Handle: []byte("test"),
		Path:   "/path",
	}

	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	// Try to get with partial handle "te"
	_, err = tree.Get([]byte("te"))
	if err != os.ErrNotExist {
		t.Errorf("Expected os.ErrNotExist for partial match, got %v", err)
	}

	// Try to get with longer handle "testing"
	_, err = tree.Get([]byte("testing"))
	if err != os.ErrNotExist {
		t.Errorf("Expected os.ErrNotExist for longer handle, got %v", err)
	}
}

func TestPrint(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Add some values
	values := []Value{
		{Handle: []byte("a"), Path: "/a"},
		{Handle: []byte("b"), Path: "/b"},
	}

	for _, v := range values {
		err = tree.Put(v)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Just test that Print doesn't panic
	tree.Print()
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first tree and add data
	tree1, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	value := Value{
		Handle: []byte("persist"),
		Path:   "/persistent/path",
	}

	err = tree1.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	tree1.Close()

	// Create second tree on same directory
	tree2, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree2.Close()

	// Should be able to retrieve the data
	path, err := tree2.Get([]byte("persist"))
	if err != nil {
		t.Fatal(err)
	}

	if path != "/persistent/path" {
		t.Errorf("Expected path '/persistent/path', got '%s'", path)
	}
}

func TestValueStruct(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Test with unicode path
	value := Value{
		Handle: []byte("unicode"),
		Path:   "/path/with/ünícode/文字",
	}

	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	path, err := tree.Get([]byte("unicode"))
	if err != nil {
		t.Fatal(err)
	}

	if path != "/path/with/ünícode/文字" {
		t.Errorf("Expected unicode path, got '%s'", path)
	}
}

func TestLargeHandle(t *testing.T) {
	tmpDir := t.TempDir()

	tree, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Test with large handle
	largeHandle := bytes.Repeat([]byte("x"), 1000)
	value := Value{
		Handle: largeHandle,
		Path:   "/large/handle/path",
	}

	err = tree.Put(value)
	if err != nil {
		t.Fatal(err)
	}

	path, err := tree.Get(largeHandle)
	if err != nil {
		t.Fatal(err)
	}

	if path != "/large/handle/path" {
		t.Errorf("Expected path '/large/handle/path', got '%s'", path)
	}
}
