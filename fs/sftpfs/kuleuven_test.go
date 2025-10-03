package sftpfs_test

import (
	"context"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	"gitea.icts.kuleuven.be/coz/sftp/pkg/sftp"
	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/nativefs"
	"github.com/kuleuven/vfs/fs/rootfs"
	"github.com/kuleuven/vfs/fs/sftpfs"
)

func TestKULeuvenSFTP(t *testing.T) {
	handler := &sftp.Handler{
		Username:   os.Getenv("SFTP_USER"),
		Options:    strings.Split(os.Getenv("SFTP_OPTIONS"), "+"),
		RemoteAddr: &net.TCPAddr{},
		Workdir:    "/",
	}

	ctx := context.WithValue(t.Context(), vfs.UseServerInodes, true)
	ctx = context.WithValue(ctx, vfs.Log, handler.Logger())

	root := rootfs.New(ctx)
	root.MustMount("/", nativefs.New(ctx, t.TempDir()), 0)

	handler.FS = root

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

	go func() {
		defer rw.Close()

		handler.Serve(ctx, rw)
	}()

	fs, err := sftpfs.NewPipe(r2, w1)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRW(t, fs)
}
