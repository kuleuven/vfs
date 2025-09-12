package wrapfs

import (
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/fs/emptyfs"
	"github.com/kuleuven/vfs/fs/errorfs"
)

var _ vfs.FS = (*Wrapdir)(nil)

type Wrapdir struct {
	FS   vfs.FS
	Dirs []string
}

func Wrap(fs vfs.FS, dirs ...string) vfs.FS {
	for _, dir := range dirs {
		if !strings.HasPrefix(dir, "/") || dir == "/" || strings.Contains(dir[1:], "/") {
			return errorfs.New(os.ErrInvalid)
		}
	}

	return &Wrapdir{FS: fs, Dirs: dirs}
}

func (s *Wrapdir) BuildPath(path string) string {
	if slices.Contains(s.Dirs, path) {
		return "/"
	}

	for _, dir := range s.Dirs {
		if strings.HasPrefix(path, dir+"/") {
			return strings.TrimPrefix(path, dir)
		}
	}

	panic("invalid path")
}

func (s *Wrapdir) IsRoot(path string) bool {
	return path == "" || path == "/"
}

func (s *Wrapdir) IsValid(path string) bool {
	if s.IsRoot(path) || slices.Contains(s.Dirs, path) {
		return true
	}

	for _, dir := range s.Dirs {
		if strings.HasPrefix(path, dir+"/") {
			return true
		}
	}

	return false
}

func (s *Wrapdir) ExtractPath(path, dir string) string {
	if s.IsRoot(path) {
		return dir
	}

	return dir + path
}

func (s *Wrapdir) Stat(path string) (vfs.FileInfo, error) {
	if s.IsRoot(path) {
		return emptyfs.EmptyDirStat{}, nil
	}

	if !s.IsValid(path) {
		return nil, os.ErrNotExist
	}

	return s.FS.Stat(s.BuildPath(path))
}

func (s *Wrapdir) List(path string) (vfs.ListerAt, error) {
	if s.IsRoot(path) {
		finfo, err := s.FS.Stat("/")

		var lister vfs.FileInfoListerAt

		for _, dir := range s.Dirs {
			lister = append(lister, &virtualdir{
				name:     dir[1:],
				FileInfo: finfo,
			})
		}

		return lister, err
	}

	if !s.IsValid(path) {
		return nil, os.ErrNotExist
	}

	return s.FS.List(s.BuildPath(path))
}

type virtualdir struct {
	name string
	vfs.FileInfo
}

func (f *virtualdir) Name() string { return f.name }

func (s *Wrapdir) FileRead(path string) (vfs.ReaderAt, error) {
	if !s.IsValid(path) {
		return nil, os.ErrNotExist
	}

	return s.FS.FileRead(s.BuildPath(path))
}

func (s *Wrapdir) FileWrite(path string, flags int) (vfs.WriterAt, error) {
	if !s.IsValid(path) {
		return nil, os.ErrNotExist
	}

	return s.FS.FileWrite(s.BuildPath(path), flags)
}

func (s *Wrapdir) Chmod(path string, mode os.FileMode) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.Chmod(s.BuildPath(path), mode)
}

func (s *Wrapdir) Chown(path string, uid, gid int) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.Chown(s.BuildPath(path), uid, gid)
}

func (s *Wrapdir) Chtimes(path string, atime, mtime time.Time) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.Chtimes(s.BuildPath(path), atime, mtime)
}

func (s *Wrapdir) Truncate(path string, size int64) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.Truncate(s.BuildPath(path), size)
}

func (s *Wrapdir) SetExtendedAttr(path, name string, value []byte) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.SetExtendedAttr(s.BuildPath(path), name, value)
}

func (s *Wrapdir) UnsetExtendedAttr(path, name string) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.UnsetExtendedAttr(s.BuildPath(path), name)
}

func (s *Wrapdir) SetExtendedAttrs(path string, attrs vfs.Attributes) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return vfs.SetExtendedAttrs(s.FS, s.BuildPath(path), attrs)
}

func (s *Wrapdir) Rename(oldpath, newpath string) error {
	if s.IsRoot(oldpath) {
		return os.ErrPermission
	}

	if !s.IsValid(oldpath) {
		return os.ErrNotExist
	}

	if s.IsRoot(newpath) {
		return os.ErrPermission
	}

	if !s.IsValid(newpath) {
		return os.ErrPermission
	}

	return s.FS.Rename(s.BuildPath(oldpath), s.BuildPath(newpath))
}

func (s *Wrapdir) Rmdir(path string) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.Rmdir(s.BuildPath(path))
}

func (s *Wrapdir) Remove(path string) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.Remove(s.BuildPath(path))
}

func (s *Wrapdir) Mkdir(path string, perm os.FileMode) error {
	if s.IsRoot(path) {
		return os.ErrPermission
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	return s.FS.Mkdir(s.BuildPath(path), perm)
}

func (s *Wrapdir) Close() error {
	return s.FS.Close()
}

func (s *Wrapdir) Walk(path string, walkFn vfs.WalkFunc) error {
	if s.IsRoot(path) {
		return s.walkRoot(walkFn)
	}

	if !s.IsValid(path) {
		return os.ErrNotExist
	}

	dir := "/" + strings.SplitN(path, "/", 3)[1]

	return vfs.Walk(s.FS, s.BuildPath(path), func(path string, info vfs.FileInfo, err error) error {
		return walkFn(s.ExtractPath(path, dir), info, err)
	})
}

func (s *Wrapdir) walkRoot(walkFn vfs.WalkFunc) error {
	err := walkFn("/", emptyfs.EmptyDirStat{}, nil)
	if err == vfs.SkipAll || err == vfs.SkipDir {
		return nil
	}

	if err == vfs.SkipSubDirs {
		finfo, err := s.FS.Stat("/")

		for _, dir := range s.Dirs {
			if err1 := walkFn(dir, finfo, err); err1 != vfs.SkipAll && err1 != vfs.SkipDir && err1 != vfs.SkipSubDirs && err1 != nil {
				return err1
			}
		}

		return nil
	}

	if err != nil {
		return err
	}

	return vfs.Walk(s.FS, "/", func(path string, info vfs.FileInfo, err error) error {
		for _, dir := range s.Dirs {
			err = walkFn(s.ExtractPath(path, dir), info, err)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
