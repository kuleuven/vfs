package iofs

import (
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/spf13/afero"
)

func TestIOFS(t *testing.T) {
	vfs.RunTestSuiteRO(t, New(afero.NewIOFS(afero.NewOsFs())))
}
