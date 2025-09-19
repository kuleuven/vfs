package nativefs

import (
	"os"
	"testing"

	"github.com/kuleuven/vfs/runas"
	"github.com/kuleuven/vfs/testsuite"
)

func TestNativeFS(t *testing.T) {
	testsuite.RunTestSuiteAdvanced(t, New(t.Context(), t.TempDir()))

	if os.Getuid() == 0 {
		dir := t.TempDir()

		if os.Chown(dir, 1000, 1000) != nil {
			return
		}

		// Allow parent dir
		if os.Chmod(dir+"/..", 0o775) != nil {
			return
		}

		testsuite.RunTestSuiteAdvanced(t, NewAsUser(t.Context(), dir, &runas.User{UID: 1000, GID: 1000}))
	}
}
