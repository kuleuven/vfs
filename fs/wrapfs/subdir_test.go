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

	vfs.RunTestSuiteRW(t, Sub(top, "/subdir"))
}
