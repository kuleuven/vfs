package irodsfs

import (
	"os"
	"time"

	"github.com/kuleuven/vfs"
)

var _ vfs.FileInfo = &fileInfo{}

// FileInfo implements os.FileInfo for a Cloud Storage file.
type fileInfo struct {
	name          string
	sizeInBytes   int64
	modTime       time.Time
	mode          os.FileMode
	extendedAttrs vfs.Attributes
	permissionSet *vfs.Permissions
	owner         int
	group         int
	sys           interface{}
}

// Name provides the base name of the file.
func (fi *fileInfo) Name() string {
	return fi.name
}

// Size provides the length in bytes for a file.
func (fi *fileInfo) Size() int64 {
	return fi.sizeInBytes
}

// Mode provides the file mode bits
func (fi *fileInfo) Mode() os.FileMode {
	return fi.mode
}

// ModTime provides the last modification time.
func (fi *fileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir provides the abbreviation for Mode().IsDir()
func (fi *fileInfo) IsDir() bool {
	return fi.mode&os.ModeDir != 0
}

// SetMode sets the file mode
func (fi *fileInfo) SetMode(mode os.FileMode) {
	fi.mode = mode
}

// Sys provides the underlying data source (can return nil)
func (fi *fileInfo) Sys() interface{} {
	return fi.sys
}

// Uid returns the uid
func (fi *fileInfo) Uid() uint32 { //nolint:staticcheck
	return uint32(fi.owner)
}

// Gid returns the gid
func (fi *fileInfo) Gid() uint32 { //nolint:staticcheck
	return uint32(fi.group)
}

func (fi *fileInfo) NumLinks() uint64 {
	return 1
}

// ExtendedAttrs returns extended attributes (if any)
func (fi *fileInfo) Extended() (vfs.Attributes, error) {
	return fi.extendedAttrs, nil
}

func (fi *fileInfo) Permissions() (*vfs.Permissions, error) {
	return fi.permissionSet, nil
}

func permissionSet(perm Permission) *vfs.Permissions {
	return &vfs.Permissions{
		Read:             perm.Includes(Read),
		Write:            perm.Includes(Write),
		Delete:           perm.Includes(Delete),
		Own:              perm.Includes(Own),
		GetExtendedAttrs: true,
		SetExtendedAttrs: perm.Includes(Write),
	}
}
