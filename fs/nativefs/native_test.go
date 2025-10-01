package nativefs

import (
	"os"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/runas"
)

func TestNativeFS(t *testing.T) {
	fs := New(t.Context(), t.TempDir())

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRW(t, New(t.Context(), t.TempDir()))
}

func TestNativeFSAsUser(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("not running as root")

		return
	}

	dir := t.TempDir()

	if os.Chown(dir, 1000, 1000) != nil {
		return
	}

	// Allow parent dir
	if os.Chmod(dir+"/..", 0o775) != nil {
		return
	}

	fs := NewAsUser(t.Context(), dir, &runas.User{UID: 1000, GID: 1000})

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRW(t, fs)
}
