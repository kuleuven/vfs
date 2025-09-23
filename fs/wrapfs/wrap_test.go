package wrapfs

import (
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestWrap(t *testing.T) {
	top := nativefs.New(t.Context(), t.TempDir())

	vfs.RunTestSuiteAdvanced(t, Sub(Wrap(top, "/a", "/b", "/c"), "/a"))
}
