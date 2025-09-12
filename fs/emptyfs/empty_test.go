package emptyfs

import (
	"testing"

	"github.com/kuleuven/vfs"
)

func TestEmptyFS(t *testing.T) {
	vfs.RunTestSuite(t, New())
}
