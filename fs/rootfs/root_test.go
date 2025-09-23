package rootfs

import (
	"context"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/emptyfs"
	"github.com/kuleuven/vfs/fs/nativefs"
)

func TestNativeFS(t *testing.T) {
	root := New(t.Context())

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

	vfs.RunTestSuiteAdvanced(t, root)
}

func TestNativeFSServerInodes(t *testing.T) {
	root := New(context.WithValue(t.Context(), vfs.UseServerInodes, true))

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

	vfs.RunTestSuiteAdvanced(t, root)
}
