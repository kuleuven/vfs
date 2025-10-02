package wrapfs

import (
	"context"
	"os"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestSubdir(t *testing.T) {
	dir := t.TempDir()

	if err := os.Mkdir(dir+"/subdir", 0o755); err != nil {
		t.Fatal(err)
	}

	ctx := context.WithValue(t.Context(), vfs.UseServerInodes, true)

	top := nativefs.New(ctx, dir).(vfs.AdvancedFS) //nolint:forcetypeassert
	sub := AdvancedSub(top, "/subdir")

	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRW(t, sub)
}
