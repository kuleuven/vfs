package wrapfs

import (
	"os"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestSubdir(t *testing.T) {
	dir := t.TempDir()

	if err := os.Mkdir(dir+"/subdir", 0o755); err != nil {
		t.Fatal(err)
	}

	top := nativefs.New(t.Context(), dir)
	sub := Sub(top, "/subdir")

	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRW(t, sub)
}
