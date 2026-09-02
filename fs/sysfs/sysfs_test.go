package sysfs

import (
	"crypto"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/kuleuven/vfs"
)

const configName = "config"

func TestSysFS(t *testing.T) {
	top := Directory{
		Name: "/",
	}

	fs := New(top)

	defer func() {
		if err := fs.Close(); err != nil {
			t.Error(err)
		}
	}()

	vfs.RunTestSuiteRO(t, fs)
}

func TestFSFileOperations(t *testing.T) {
	fs := New(Directory{
		Name: "/",
		Directories: []Directory{{
			Name:    "etc",
			Entries: []Entry{{Name: configName, Payload: []byte("hello")}},
		}},
	})

	info, err := fs.Stat("/etc/config")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Name() != configName || info.Size() != 5 {
		t.Fatalf("unexpected file info: name=%q size=%d", info.Name(), info.Size())
	}

	file, err := fs.Open("/etc/config")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}()

	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(data) != "hello" {
		t.Fatalf("unexpected contents: %q", data)
	}

	list, err := fs.List("/etc")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	infos := make([]vfs.FileInfo, 1)
	if n, err := list.ListAt(infos, 0); n != 1 || err != nil {
		t.Fatalf("ListAt returned (%d, %v)", n, err)
	}

	if infos[0].Name() != configName {
		t.Fatalf("unexpected listed name: %q", infos[0].Name())
	}

	handle, err := fs.Handle("/etc/config")
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	resolved, err := fs.Path(handle)
	if err != nil {
		t.Fatalf("Path failed: %v", err)
	}

	if resolved != "/etc/config" {
		t.Fatalf("unexpected resolved path: %q", resolved)
	}

	if _, err := fs.Stat("relative"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat(relative) error = %v, want os.ErrNotExist", err)
	}
}

func TestFSChecksum(t *testing.T) {
	fs := New(Directory{
		Name: "/",
		Entries: []Entry{{
			Name:    configName,
			Payload: []byte("hello"),
		}},
	})

	checksum, err := fs.Checksum("/config", crypto.SHA256)
	if err != nil {
		t.Fatalf("Checksum failed: %v", err)
	}

	if len(checksum) != crypto.SHA256.Size() {
		t.Fatalf("unexpected checksum length: %d", len(checksum))
	}

	if _, err := fs.Checksum("/config", crypto.Hash(0)); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("unsupported checksum error = %v, want os.ErrInvalid", err)
	}
}

func TestFSRootFSOperations(t *testing.T) {
	fs := New(Directory{
		Name: "/",
		Entries: []Entry{{
			Name:    configName,
			Payload: []byte("hello"),
		}},
	})

	reader, err := fs.FileRead("/config")
	if err != nil {
		t.Fatalf("FileRead failed: %v", err)
	}
	defer reader.Close()

	file, err := fs.OpenFile("/config", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile read-only failed: %v", err)
	}
	defer file.Close()

	if _, err := fs.OpenFile("/config", os.O_WRONLY, 0); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("OpenFile write error = %v, want os.ErrPermission", err)
	}

	path, err := fs.RealPath("/config")
	if err != nil || path != "/config" {
		t.Fatalf("RealPath returned (%q, %v), want (/config, nil)", path, err)
	}

	if _, err := fs.FileWrite("/config", os.O_WRONLY); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("FileWrite error = %v, want os.ErrPermission", err)
	}

	if err := fs.Chtimes("/config", time.Time{}, time.Time{}); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Chtimes error = %v, want os.ErrPermission", err)
	}

	if err := fs.Symlink("/config", "/link"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Symlink error = %v, want os.ErrPermission", err)
	}
}
