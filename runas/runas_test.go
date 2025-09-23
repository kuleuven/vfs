package runas

import (
	"os"
	"testing"
)

func TestRunAs(t *testing.T) {
	t.Run("RunAsCurrentUser", func(t *testing.T) {
		testRunAs(RunAsCurrentUser())
	})

	if os.Getuid() == 0 {
		t.Run("RunAs", func(t *testing.T) {
			ctx, err := RunAs(&User{UID: 1000, GID: 1000})
			if err != nil {
				t.Fatal(err)
			}

			testRunAs(ctx)
		})
	}
}

func testRunAs(ctx Context) {
	defer ctx.Close()

	ctx.Run(func() error {
		return nil
	})
}
