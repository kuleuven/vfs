package writeonlyfs

import (
	"context"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestWriteOnlyFS(t *testing.T) {
	ctx := context.WithValue(t.Context(), AllowRemove, true)

	parent := nativefs.New(ctx, t.TempDir())

	if err := parent.Mkdir("/tmp", 0o755); err != nil {
		t.Fatal(err)
	}

	fs := NewAt(ctx, parent, "/tmp", "test")

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRW(t, fs)
}
