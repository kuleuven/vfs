package vfs_test

import (
	"io"
	"os"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestDerived(t *testing.T) {
	fs := nativefs.New(t.Context(), t.TempDir())

	defer fs.Close()

	if err := vfs.MkdirAll(fs, "/tmp/nested/deep", 0o755); err != nil {
		t.Error(err)
	}

	if err := vfs.WriteFile(fs, "/tmp/test.txt", []byte("test"), os.O_CREATE|os.O_WRONLY); err != nil {
		t.Error(err)
	}

	f, err := vfs.Open(fs, "/tmp/test.txt")
	if err != nil {
		t.Error(err)
	}

	if _, err := io.ReadAll(f); err != nil {
		t.Error(err)
	}

	if err := f.Close(); err != nil {
		t.Error(err)
	}

	if err := vfs.RemoveAll(fs, "/"); err != nil {
		t.Error(err)
	}
}
