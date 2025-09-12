package vfs

import (
	"errors"
	"io"
	"testing"
)

func RunTestSuite(t *testing.T, fs FS) {
	t.Run("Stat", func(t *testing.T) {
		finfo, err := fs.Stat("/")
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%s", finfo.Name())
	})

	t.Run("List", func(t *testing.T) {
		lister, err := fs.List("/")
		if err != nil {
			t.Fatal(err)
		}

		buf := make([]FileInfo, 10)

		n, err := lister.ListAt(buf, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}

		for _, finfo := range buf[:n] {
			t.Logf("%s", finfo.Name())
		}
	})

	t.Run("Walk", func(t *testing.T) {
		err := Walk(fs, "/", func(path string, info FileInfo, err error) error {
			t.Logf("%s", path)

			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}
