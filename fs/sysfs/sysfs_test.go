package sysfs

import (
	"testing"

	"github.com/kuleuven/vfs"
)

func TestSysFS(t *testing.T) {
	top := Directory{
		Name: "/",
	}

	fs := New(top)

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRO(t, fs)
}
