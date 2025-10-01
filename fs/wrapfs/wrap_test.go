package wrapfs

import (
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestWrap(t *testing.T) {
	top := nativefs.New(t.Context(), t.TempDir())
	sub := Sub(Wrap(top, "/a", "/b", "/c"), "/a")

	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRW(t, sub)
}
