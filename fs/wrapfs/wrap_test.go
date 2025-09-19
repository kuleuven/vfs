package wrapfs

import (
	"testing"

	"github.com/kuleuven/vfs/fs/nativefs"
	"github.com/kuleuven/vfs/testsuite"
)

func TestWrap(t *testing.T) {
	top := nativefs.New(t.Context(), t.TempDir())

	testsuite.RunTestSuiteAdvanced(t, Sub(Wrap(top, "/a", "/b", "/c"), "/a"))
}
