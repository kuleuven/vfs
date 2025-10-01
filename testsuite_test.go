package vfs_test

import (
	"context"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
	"github.com/kuleuven/vfs/fs/rootfs"
)

func TestNativeFS(t *testing.T) {
	fs := nativefs.New(t.Context(), t.TempDir())

	defer fs.Close()

	vfs.RunTestSuiteRW(t, fs)
}

func TestRootFS(t *testing.T) {
	ctx := context.WithValue(t.Context(), vfs.PersistentStorage, t.TempDir())
	root := rootfs.New(ctx)

	root.MustMount("/", nativefs.New(ctx, t.TempDir()), 0)

	defer root.Close()

	vfs.RunTestSuiteRW(t, root)
}
