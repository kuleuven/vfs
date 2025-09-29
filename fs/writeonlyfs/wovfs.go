package writeonlyfs

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/wrapfs"
	"github.com/sirupsen/logrus"
)

var _ vfs.FS = (*wrap)(nil)

var FileVisibilityTimeout = 45 * time.Minute

var AllowRemove vfs.ContextKey = "wofs-allow-remove"

// New creates a write-only virtual filesystem,
// based on the parent filesystem.
// The given username will be stored as metadata.
// Clients will only be able to list files that are uploaded within the same session.
func New(ctx context.Context, parent vfs.FS, username string) vfs.FS {
	return &wrap{
		orig:        parent,
		myfiles:     []string{"/"},
		user:        username,
		allowRemove: vfs.Bool(ctx, AllowRemove),
	}
}

// New creates a write-only virtual filesystem
// based on the parent filesystem at the given path.
// The given username will be stored as metadata.
// Clients will only be able to list files that are uploaded within the same session.
func NewAt(ctx context.Context, parent vfs.FS, at, username string) vfs.FS {
	at = strings.TrimSuffix(at, "/")

	if at != "" {
		parent = wrapfs.Sub(parent, at)
	}

	return New(ctx, parent, username)
}

type wrap struct {
	vfs.NotImplementedFS
	orig        vfs.FS
	myfiles     []string // Files that I "know" about because the same process has opened them
	user        string   // Username to store as metadata
	allowRemove bool
}

func (w *wrap) FileRead(path string) (vfs.ReaderAt, error) {
	// Allow to read any of our own uploaded files
	// Meaning that the same connection did the upload
	if slices.Contains(w.myfiles, path) {
		return w.orig.FileRead(path)
	}

	_, err := w.orig.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}

	return nil, os.ErrPermission
}

func (w *wrap) FileWrite(path string, flag int) (vfs.WriterAt, error) {
	if slices.Contains(w.myfiles, path) {
		return w.orig.FileWrite(path, flag)
	}

	fi, err := w.orig.Stat(path)

	switch {
	case err == nil:
		ext, err := fi.Extended()
		if err != nil {
			return nil, os.ErrPermission
		}

		// Only accept overwriting files if it is our own file
		if GetAttr(ext, userMeta) != w.user {
			return nil, os.ErrPermission
		}

		if ts, err := time.Parse(time.RFC3339, GetAttr(ext, timestampMeta)); err != nil || time.Since(ts) > FileVisibilityTimeout {
			return nil, os.ErrPermission
		}

	case errors.Is(err, os.ErrNotExist):
		// Make sure metadata is set after the Filewrite below has created the file
		defer w.orig.SetExtendedAttr(path, userMeta, []byte(w.user))                               //nolint:errcheck
		defer w.orig.SetExtendedAttr(path, timestampMeta, []byte(time.Now().Format(time.RFC3339))) //nolint:errcheck
	default:
		return nil, err
	}

	w.myfiles = append(w.myfiles, path)

	return w.orig.FileWrite(path, flag)
}

const userMeta = "user.meta.mg.ingest.user"

const timestampMeta = "user.meta.mg.ingest.timestamp"

func (w *wrap) Chmod(path string, mode os.FileMode) error {
	if slices.Contains(w.myfiles, path) {
		return w.orig.Chmod(path, mode)
	}

	return os.ErrPermission
}

func (w *wrap) Chtimes(path string, atime, mtime time.Time) error {
	if slices.Contains(w.myfiles, path) {
		return w.orig.Chtimes(path, atime, mtime)
	}

	return os.ErrPermission
}

func (w *wrap) Truncate(path string, size int64) error {
	if slices.Contains(w.myfiles, path) {
		return w.orig.Truncate(path, size)
	}

	// Never overwrite an existing file
	_, err := w.orig.Stat(path)
	if !errors.Is(err, os.ErrNotExist) {
		return os.ErrPermission
	}

	// Append own metadata
	defer w.orig.SetExtendedAttr(path, userMeta, []byte(w.user))                               //nolint:errcheck
	defer w.orig.SetExtendedAttr(path, timestampMeta, []byte(time.Now().Format(time.RFC3339))) //nolint:errcheck

	w.myfiles = append(w.myfiles, path)

	return w.orig.Truncate(path, size)
}

func (w *wrap) Mkdir(path string, perm os.FileMode) error {
	if slices.Contains(w.myfiles, path) {
		return os.ErrExist
	}

	// Only accept if the directory does not exist or is already a directory that we couldn't list
	fi, err := w.orig.Stat(path)
	if err == nil && fi.IsDir() {
		w.myfiles = append(w.myfiles, path)

		return nil
	}

	if err == nil {
		return os.ErrExist
	}

	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	w.myfiles = append(w.myfiles, path)

	return w.orig.Mkdir(path, perm)
}

func (w *wrap) Stat(path string) (vfs.FileInfo, error) {
	s, err := w.orig.Stat(path)
	if errors.Is(err, os.ErrNotExist) { // If the file does not exist, return the error
		return nil, err
	}

	// If we know the file in our current process, return the stat result including possible errors
	if slices.Contains(w.myfiles, path) {
		return s, err
	}

	// If an error occurred for an unknown file, hide the error
	if err != nil {
		logrus.Debugf("Hiding error for file %s: %v", path, err)

		return nil, os.ErrPermission
	}

	if s.IsDir() {
		return s, nil
	}

	// Check for metadata
	ext, err := s.Extended()
	if err != nil {
		return nil, os.ErrPermission
	}

	if GetAttr(ext, userMeta) != w.user {
		logrus.Debugf("File %s is not owned by us: %s", path, GetAttr(ext, userMeta))

		return nil, os.ErrPermission
	}

	if ts, err := time.Parse(time.RFC3339, GetAttr(ext, timestampMeta)); err != nil || time.Since(ts) > FileVisibilityTimeout {
		logrus.Debugf("File %s is too old: %v, %s", path, err, ts)

		return nil, os.ErrPermission
	}

	return s, nil
}

func (w *wrap) List(path string) (vfs.ListerAt, error) {
	l, err := List(w.orig, path)
	if err != nil {
		return nil, err
	}

	f := make([]vfs.FileInfo, 0, len(l))

	for _, fi := range l {
		name := path + "/" + fi.Name()

		if strings.HasSuffix(path, "/") {
			name = path + fi.Name()
		}

		if slices.Contains(w.myfiles, name) {
			f = append(f, fi)

			continue
		}

		ext, err := fi.Extended()
		if err != nil {
			continue
		}

		if GetAttr(ext, userMeta) != w.user {
			continue
		}

		if ts, err := time.Parse(time.RFC3339, GetAttr(ext, timestampMeta)); err != nil || time.Since(ts) > FileVisibilityTimeout {
			continue
		}

		f = append(f, fi)
	}

	return vfs.FileInfoListerAt(f), nil
}

func (w *wrap) Rename(oldpath, newpath string) error {
	// The old file must be visible, otherwice we can't rename it
	if _, err := w.Stat(oldpath); err != nil {
		return err
	}

	// Allow to overwrite any of our files
	if slices.Contains(w.myfiles, newpath) {
		return w.rename(oldpath, newpath)
	}

	// Check if the new file exists
	_, err := w.orig.Stat(newpath)
	if errors.Is(err, os.ErrNotExist) {
		return w.rename(oldpath, newpath)
	} else if err != nil {
		logrus.Debugf("Hiding error for rename of %s: %v", oldpath, err)

		return os.ErrPermission
	}

	logrus.Debugf("File %s already exists", newpath)

	// Don't overwrite any data, even if it was uploaded in the last ten minutes
	return os.ErrPermission
}

func (w *wrap) rename(oldpath, newpath string) error {
	if err := w.orig.Rename(oldpath, newpath); err != nil {
		return err
	}

	for i, v := range w.myfiles {
		if v == oldpath {
			w.myfiles[i] = newpath
		}
	}

	return nil
}

func (w *wrap) Remove(path string) error {
	// The old file must be visible, otherwice we can't remove it
	if _, err := w.Stat(path); err != nil {
		return err
	}

	if !w.allowRemove {
		return os.ErrPermission
	}

	return w.orig.Remove(path)
}

func GetAttr(m vfs.Attributes, key string) string {
	if m == nil {
		return ""
	}

	v, ok := m[key]
	if ok {
		return string(v)
	}

	return ""
}

func (w *wrap) Close() error {
	return w.orig.Close()
}

func (w *wrap) SetExtendedAttr(path, name string, value []byte) error {
	if _, err := w.Stat(path); err != nil {
		return err
	}

	if name == userMeta || name == timestampMeta {
		return os.ErrPermission
	}

	return w.orig.SetExtendedAttr(path, name, value)
}

func (w *wrap) UnsetExtendedAttr(path, name string) error {
	if _, err := w.Stat(path); err != nil {
		return err
	}

	if name == userMeta || name == timestampMeta {
		return os.ErrPermission
	}

	return w.orig.UnsetExtendedAttr(path, name)
}

func (w *wrap) SetExtendedAttrs(path string, attrs vfs.Attributes) error {
	fi, err := w.Stat(path)
	if err != nil {
		return err
	}

	current, err := fi.Extended()
	if err != nil {
		return err
	}

	user, ok := current.Get(userMeta)
	if !ok {
		return os.ErrPermission
	}

	attrs.Set(userMeta, user)

	timestamp, ok := current.Get(timestampMeta)
	if !ok {
		return os.ErrPermission
	}

	attrs.Set(timestampMeta, timestamp)

	return vfs.SetExtendedAttrs(w.orig, path, attrs)
}
