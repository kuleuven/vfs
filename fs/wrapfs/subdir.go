package wrapfs

import (
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/kuleuven/vfs"
)

var _ vfs.FS = &subdir{}

// Sub returns a FS that is a subdirectory of the given parent FS.
func Sub(parent vfs.FS, dir string) vfs.FS {
	if dir == "" || dir == "/" {
		return parent
	}

	return &subdir{Parent: parent, Dir: dir}
}

func AdvancedSub(parent vfs.AdvancedFS, dir string) vfs.AdvancedFS {
	if dir == "" || dir == "/" {
		return parent
	}

	return &subdirAdvanced{
		subdir: &subdir{
			Parent: parent,
			Dir:    dir,
		},
		Parent: parent,
	}
}

func AdvancedLinkSub(parent vfs.AdvancedLinkFS, dir string) vfs.AdvancedLinkFS {
	return &subdirAdvancedLink{
		subdirAdvanced: &subdirAdvanced{
			subdir: &subdir{
				Parent: parent,
				Dir:    dir,
			},
			Parent: parent,
		},
		Parent: parent,
	}
}

func Parent(parent vfs.FS) vfs.FS {
	switch v := parent.(type) {
	case *subdir:
		return v.Parent
	case *subdirAdvanced:
		return v.Parent
	case *subdirAdvancedLink:
		return v.Parent
	default:
		return nil
	}
}

func ParentAdvanced(parent vfs.AdvancedFS) vfs.AdvancedFS {
	switch v := parent.(type) {
	case *subdirAdvanced:
		return v.Parent
	case *subdirAdvancedLink:
		return v.Parent
	default:
		return nil
	}
}

func ParentAdvancedLink(parent vfs.AdvancedLinkFS) vfs.AdvancedLinkFS {
	switch v := parent.(type) {
	case *subdirAdvancedLink:
		return v.Parent
	default:
		return nil
	}
}

type subdir struct {
	Parent vfs.FS
	Dir    string
}

type subdirAdvanced struct {
	*subdir
	Parent vfs.AdvancedFS
}

type subdirAdvancedLink struct {
	*subdirAdvanced
	Parent vfs.AdvancedLinkFS
}

func (s *subdir) buildPath(path string) string {
	if path == "" || path == "/" {
		return s.Dir
	}

	return s.Dir + path
}

func (s *subdir) extractPath(path string) string {
	if path == s.Dir {
		return "/"
	}

	return strings.TrimPrefix(path, s.Dir)
}

func (s *subdir) extractPathCheck(path string) (string, error) {
	if path == s.Dir {
		return "/", nil
	}

	if !strings.HasPrefix(path, s.Dir+"/") {
		return "", syscall.EINVAL
	}

	return strings.TrimPrefix(path, s.Dir), nil
}

func (s *subdir) Stat(path string) (vfs.FileInfo, error) {
	return s.Parent.Stat(s.buildPath(path))
}

func (s *subdir) List(path string) (vfs.ListerAt, error) {
	return s.Parent.List(s.buildPath(path))
}

func (s *subdir) FileRead(path string) (vfs.ReaderAt, error) {
	return s.Parent.FileRead(s.buildPath(path))
}

func (s *subdir) FileWrite(path string, flags int) (vfs.WriterAt, error) {
	return s.Parent.FileWrite(s.buildPath(path), flags)
}

func (s *subdir) Chmod(path string, mode os.FileMode) error {
	return s.Parent.Chmod(s.buildPath(path), mode)
}

func (s *subdir) Chown(path string, uid, gid int) error {
	return s.Parent.Chown(s.buildPath(path), uid, gid)
}

func (s *subdir) Chtimes(path string, atime, mtime time.Time) error {
	return s.Parent.Chtimes(s.buildPath(path), atime, mtime)
}

func (s *subdir) Truncate(path string, size int64) error {
	return s.Parent.Truncate(s.buildPath(path), size)
}

func (s *subdir) SetExtendedAttr(path, name string, value []byte) error {
	return s.Parent.SetExtendedAttr(s.buildPath(path), name, value)
}

func (s *subdir) UnsetExtendedAttr(path, name string) error {
	return s.Parent.UnsetExtendedAttr(s.buildPath(path), name)
}

func (s *subdir) SetExtendedAttrs(path string, attrs vfs.Attributes) error {
	return vfs.SetExtendedAttrs(s.Parent, s.buildPath(path), attrs)
}

func (s *subdir) Rename(oldpath, newpath string) error {
	return s.Parent.Rename(s.buildPath(oldpath), s.buildPath(newpath))
}

func (s *subdir) Rmdir(path string) error {
	return s.Parent.Rmdir(s.buildPath(path))
}

func (s *subdir) Remove(path string) error {
	return s.Parent.Remove(s.buildPath(path))
}

func (s *subdir) Mkdir(path string, perm os.FileMode) error {
	return s.Parent.Mkdir(s.buildPath(path), perm)
}

func (s *subdir) Close() error {
	return s.Parent.Close()
}

func (s *subdir) Walk(path string, walkFn vfs.WalkFunc) error {
	return vfs.Walk(s.Parent, s.buildPath(path), func(path string, info vfs.FileInfo, err error) error {
		return walkFn(s.extractPath(path), info, err)
	})
}

func (s *subdirAdvanced) Handle(path string) ([]byte, error) {
	return s.Parent.Handle(s.buildPath(path))
}

func (s *subdirAdvanced) Path(handle []byte) (string, error) {
	path, err := s.Parent.Path(handle)
	if err != nil {
		return "", err
	}

	return s.extractPath(path), nil
}

func (s *subdirAdvanced) OpenFile(path string, mode int, perm os.FileMode) (vfs.File, error) {
	f, err := s.Parent.OpenFile(s.buildPath(path), mode, perm)
	if err != nil {
		return nil, err
	}

	return &adjustName{s, f}, nil
}

type adjustName struct {
	sda *subdirAdvanced
	vfs.File
}

func (a *adjustName) Name() string {
	return a.sda.extractPath(a.File.Name())
}

func (s *subdirAdvancedLink) Lstat(path string) (vfs.FileInfo, error) {
	return s.Parent.Lstat(s.buildPath(path))
}

func (s *subdirAdvancedLink) Readlink(path string) (string, error) {
	target, err := s.Parent.Readlink(s.buildPath(path))
	if err != nil {
		return "", err
	}

	return s.extractPathCheck(target)
}

func (s *subdirAdvancedLink) RealPath(path string) (string, error) {
	target, err := s.Parent.RealPath(s.buildPath(path))
	if err != nil {
		return "", err
	}

	return s.extractPathCheck(target)
}

func (s *subdirAdvancedLink) Link(oldname, newname string) error {
	return s.Parent.Link(s.buildPath(oldname), s.buildPath(newname))
}

func (s *subdirAdvancedLink) Symlink(oldname, newname string) error {
	return s.Parent.Symlink(s.buildPath(oldname), s.buildPath(newname))
}
