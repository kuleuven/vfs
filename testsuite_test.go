package vfs_test

import (
	"context"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
	"github.com/kuleuven/vfs/fs/rootfs"
)

func TestNativeFS(t *testing.T) {
	vfs.RunTestSuiteRW(t, nativefs.New(t.Context(), t.TempDir()))
}

func TestRootFS(t *testing.T) {
	ctx := context.WithValue(t.Context(), vfs.PersistentStorage, t.TempDir())
	root := rootfs.New(ctx)

	root.MustMount("/", nativefs.New(ctx, t.TempDir()), 0)

	vfs.RunTestSuiteRW(t, root)
}
