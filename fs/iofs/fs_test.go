package iofs

import (
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/spf13/afero"
)

func TestIOFS(t *testing.T) {
	fs := New(afero.NewIOFS(afero.NewOsFs()))

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRO(t, fs)
}
