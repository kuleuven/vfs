package irodsfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/kuleuven/iron/api"
	"github.com/kuleuven/vfs"
)

func (fs *IRODS) openFile(name string, flags int) (*IRODSFileHandle, error) {
	dataobject, err := fs.Client.GetDataObject(fs.Context, name)
	if errors.Is(err, api.ErrNoRowFound) {
		dataobject = nil
	} else if err != nil {
		return nil, err
	}

	if flags&os.O_EXCL != 0 && dataobject != nil {
		return nil, syscall.EEXIST
	}

	if flags&os.O_CREATE == 0 && dataobject == nil {
		return nil, os.ErrNotExist
	}

	if flags&os.O_RDWR != 0 {
		flags &= ^os.O_WRONLY
	}

	fs.lock.Lock()
	defer fs.lock.Unlock()

	handle, err := fs.Client.OpenDataObject(fs.Context, name, flags)
	if err != nil {
		return nil, err
	}

	linearOffset, err := handle.Seek(0, io.SeekCurrent)
	if err != nil {
		defer handle.Close()

		return nil, err
	}

	fileHandle := &IRODSFileHandle{
		fs:           fs,
		handle:       handle,
		mode:         flags,
		dataobject:   dataobject,
		linearOffset: linearOffset,
	}

	fs.openFiles = append(fs.openFiles, fileHandle)

	return fileHandle, nil
}

// IRODSFileHandle is a handle for a file opened
type IRODSFileHandle struct {
	fs           *IRODS
	handle       api.File
	mode         int
	dataobject   *api.DataObject
	linearOffset int64
	sync.Mutex
}

func (handle *IRODSFileHandle) Name() string {
	return handle.handle.Name()
}

func (handle *IRODSFileHandle) Stat() (vfs.FileInfo, error) {
	actualSize, err := handle.handle.Size()
	if err != nil {
		return nil, err
	}

	if handle.dataobject == nil {
		handle.dataobject, _ = handle.fs.Client.GetDataObject(handle.fs.Context, handle.handle.Name()) //nolint:errcheck
	}

	if handle.dataobject == nil {
		vfs.Logger(handle.fs.Context).Warnf("failed to get irods attributes: no id")

		return &fileInfo{
			name:        vfs.Base(handle.Name()),
			sizeInBytes: actualSize,
			modTime:     time.Now(),
			mode:        os.FileMode(0o640),
		}, nil
	}

	metadata, err := handle.fs.Client.ListMetadata(handle.fs.Context, handle.dataobject.Path, api.DataObjectType)
	if err != nil {
		vfs.Logger(handle.fs.Context).Warnf("failed to get irods attributes: %s", err)
	}

	access, err := handle.fs.Client.ListAccess(handle.fs.Context, handle.dataobject.Path, api.DataObjectType)
	if err != nil {
		vfs.Logger(handle.fs.Context).Warnf("failed to get irods attributes: %s", err)
	}

	attrs := handle.fs.Linearize(metadata, access)

	attrs.Set("user.irods.creator", []byte(handle.fs.Client.Env().Username))
	attrs.Set("user.irods.global_id", []byte(fmt.Sprintf("%s:%d", handle.fs.Client.Env().Zone, handle.dataobject.ID)))

	return &fileInfo{
		name:          vfs.Base(handle.Name()),
		sizeInBytes:   actualSize,
		modTime:       time.Now(),
		mode:          handle.fs.getFileMode(handle.dataobject.Replicas[0].Owner, access, false),
		owner:         handle.fs.ResolveUID(handle.fs.Username()),
		group:         handle.fs.ResolveUID(handle.dataobject.Replicas[0].Owner),
		extendedAttrs: attrs,
		permissionSet: permissionSet(handle.fs.getPermission(access, handle.fs.Username(), true)),
		sys:           handle.dataobject,
	}, nil
}

func (handle *IRODSFileHandle) IsReadMode() bool {
	return handle.mode&os.O_WRONLY == 0
}

func (handle *IRODSFileHandle) IsWriteMode() bool {
	return handle.mode&os.O_WRONLY != 0 || handle.mode&os.O_RDWR != 0
}

var ErrInvalidOffset = errors.New("invalid offset")

func (handle *IRODSFileHandle) Seek(offset int64, whence int) (int64, error) {
	handle.Lock()
	defer handle.Unlock()

	offset, err := handle.handle.Seek(offset, whence)

	handle.linearOffset = offset

	return offset, err
}

var ErrWrongMode = errors.New("file is opened with wrong mode")

func (handle *IRODSFileHandle) Read(buffer []byte) (int, error) {
	if !handle.IsReadMode() {
		return 0, ErrWrongMode
	}

	handle.Lock()
	defer handle.Unlock()

	_, err := handle.handle.Seek(handle.linearOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	n, err := handle.handle.Read(buffer)
	if n > 0 && err == io.EOF {
		err = nil
	}

	handle.linearOffset += int64(n)

	return n, err
}

func (handle *IRODSFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	if !handle.IsReadMode() {
		return 0, ErrWrongMode
	}

	handle.Lock()
	defer handle.Unlock()

	if newOffset, err := handle.handle.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	} else if newOffset != offset {
		return 0, io.EOF
	}

	n, err := handle.handle.Read(buffer)
	if n > 0 && err == io.EOF {
		err = nil
	}

	return n, err
}

func (handle *IRODSFileHandle) Write(buffer []byte) (int, error) {
	if !handle.IsWriteMode() {
		return 0, ErrWrongMode
	}

	handle.Lock()
	defer handle.Unlock()

	_, err := handle.handle.Seek(handle.linearOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	n, err := handle.handle.Write(buffer)

	handle.linearOffset += int64(n)

	return n, err
}

// WriteAt writes the file to given offset
func (handle *IRODSFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	if !handle.IsWriteMode() {
		return 0, ErrWrongMode
	}

	handle.Lock()
	defer handle.Unlock()

	if newOffset, err := handle.handle.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	} else if newOffset != offset {
		return 0, io.EOF
	}

	return handle.handle.Write(data)
}

func (handle *IRODSFileHandle) Truncate(size int64) error {
	return handle.handle.Truncate(size)
}

func (handle *IRODSFileHandle) Chtimes(atime, mtime time.Time) error {
	return handle.handle.Touch(mtime)
}

func (handle *IRODSFileHandle) Readdir(int) ([]vfs.FileInfo, error) {
	return nil, syscall.ENOTDIR
}

// Close closes the file
func (handle *IRODSFileHandle) Close() error {
	handle.fs.lock.Lock()
	defer handle.fs.lock.Unlock()

	for i, v := range handle.fs.openFiles {
		if handle == v {
			handle.fs.openFiles = append(handle.fs.openFiles[:i], handle.fs.openFiles[i+1:]...)
			break
		}
	}

	return handle.handle.Close()
}
