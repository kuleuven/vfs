package vfs_test

import (
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
	"github.com/kuleuven/vfs/fs/rootfs"
)

func TestNativeFS(t *testing.T) {
	vfs.RunTestSuiteAdvanced(t, nativefs.New(t.Context(), t.TempDir()))
}

func TestRootFS(t *testing.T) {
	root := rootfs.New(t.Context())

	root.MustMount("/", nativefs.New(t.Context(), t.TempDir()), 0)

	vfs.RunTestSuiteAdvanced(t, root)
}
