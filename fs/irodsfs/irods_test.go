package irodsfs_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"

	"gitea.icts.kuleuven.be/coz/cobalt"
	"github.com/kuleuven/iron"
	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/irodsfs"
	"github.com/kuleuven/vfs/fs/nativefs"
	"github.com/sirupsen/logrus"
)

func runServer(ctx context.Context, dir string) int {
	cert, err := tls.LoadX509KeyPair("test/cert.pem", "test/key.pem")
	if err != nil {
		logrus.Fatal(err)
	}

	fs := nativefs.New(ctx, dir).(vfs.OpenFileFS) //nolint:forcetypeassert

	config := cobalt.Config{
		Certificate: cert,
		Users: []cobalt.User{
			{
				Username: "admin",
				Password: "test",
				FS:       fs,
			},
		},
	}

	server, err := cobalt.New(config)
	if err != nil {
		logrus.Fatal(err)
	}

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		logrus.Fatal(err)
	}

	port := l.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert

	go func() {
		if err := server.Serve(ctx, l); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			logrus.Fatal(err)
		}
	}()

	return port
}

func TestIrodsFS(t *testing.T) {
	port := runServer(t.Context(), t.TempDir())

	logrus.SetLevel(logrus.DebugLevel)

	client, err := iron.New(t.Context(), iron.Env{
		Host:            "localhost",
		Port:            port,
		Username:        "admin",
		Password:        "test",
		Zone:            "cobalt",
		SSLVerifyServer: "none",
	}, iron.Option{
		ClientName:         "test-irods-fs",
		MaxConns:           2,
		AllowConcurrentUse: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	testfs := irodsfs.New(t.Context(), "cobalt", client)

	vfs.RunTestSuiteRW(t, testfs)
}
