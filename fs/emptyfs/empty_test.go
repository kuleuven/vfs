package emptyfs

import (
	"testing"

	"github.com/kuleuven/vfs/testsuite"
)

func TestEmptyFS(t *testing.T) {
	testsuite.RunTestSuiteRO(t, New())
}
