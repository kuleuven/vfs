package rootfs

import (
	"context"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/emptyfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestRootNativeFS(t *testing.T) {
	ctx := context.WithValue(t.Context(), vfs.PersistentStorage, t.TempDir())

	root := New(ctx)

	defer func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	}()

	dir := t.TempDir()

	if err := root.Mount("/", nativefs.New(t.Context(), dir), 0); err != nil {
		t.Fatal(err)
	}

	if err := root.Mount("/submount", nativefs.New(t.Context(), dir), 1); err != nil {
		t.Fatal(err)
	}

	if err := root.Mount("/empty", emptyfs.New(), 2); err != nil {
		t.Fatal(err)
	}

	vfs.RunTestSuiteRW(t, root)
}

func TestRootNativeFSServerInodes(t *testing.T) {
	ctx := context.WithValue(t.Context(), vfs.UseServerInodes, true)

	root := New(ctx)

	defer func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	}()

	dir := t.TempDir()

	if err := root.Mount("/", nativefs.New(ctx, dir), 0); err != nil {
		t.Fatal(err)
	}

	if err := root.Mount("/submount", nativefs.New(ctx, dir), 1); err != nil {
		t.Fatal(err)
	}

	if err := root.Mount("/empty", emptyfs.New(), 2); err != nil {
		t.Fatal(err)
	}

	vfs.RunTestSuiteRW(t, root)
}
