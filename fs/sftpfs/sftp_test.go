package sftpfs

import (
	"io"
	"os"
	"testing"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/wrapfs"
	"github.com/pkg/sftp"
)

func TestSFTP(t *testing.T) {
	r1, w1, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	r2, w2, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	rw := struct {
		io.Reader
		io.WriteCloser
	}{r1, w2}

	server, err := sftp.NewServer(rw)
	if err != nil {
		t.Fatal(err)
	}

	defer server.Close()

	go server.Serve()

	fs, err := NewPipe(r2, w1)
	if err != nil {
		t.Fatal(err)
	}

	sub := wrapfs.Sub(fs, t.TempDir())

	vfs.RunTestSuiteRW(t, sub)
}
