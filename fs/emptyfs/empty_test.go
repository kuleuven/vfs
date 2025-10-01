package emptyfs

import (
	"testing"

	"github.com/kuleuven/vfs"
)

func TestEmptyFS(t *testing.T) {
	fs := New()

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRO(t, fs)
}
