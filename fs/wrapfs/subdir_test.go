package wrapfs

import (
	"os"
	"testing"

	"github.com/kuleuven/vfs/fs/nativefs"
	"github.com/kuleuven/vfs/testsuite"
)

func TestSubdir(t *testing.T) {
	dir := t.TempDir()

	if err := os.Mkdir(dir+"/subdir", 0o755); err != nil {
		t.Fatal(err)
	}

	top := nativefs.New(t.Context(), dir)

	testsuite.RunTestSuiteAdvanced(t, Sub(top, "/subdir"))
}
