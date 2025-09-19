package testsuite

import (
	"testing"

	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestNativeFS(t *testing.T) {
	RunTestSuiteAdvanced(t, nativefs.New(t.Context(), t.TempDir()))
}
