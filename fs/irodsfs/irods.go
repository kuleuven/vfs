package irodsfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kuleuven/iron"
	"github.com/kuleuven/iron/api"
	"github.com/kuleuven/iron/msg"
	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/io/buffered"
	"go.uber.org/multierr"
)

var _ vfs.OpenFileFS = &IRODS{}

var _ vfs.HandleResolveFS = &IRODS{}

type IRODS struct {
	vfs.NotImplementedFS

	Client               *iron.Client
	Context              context.Context //nolint:containedctx
	ChunkSize            int
	MaxChunks            int
	OpenFileAllowedPaths []string

	openFiles  []*IRODSFileHandle
	lock       sync.Mutex
	uidCache   sync.Map
	groupCache sync.Map
}

var OpenFileAllowedPaths vfs.ContextKey = "open-file-allowed-paths"

type Option func(*IRODS)

func WithChunkSize(chunkSize int) Option {
	return func(fs *IRODS) {
		fs.ChunkSize = chunkSize
	}
}

func WithMaxChunks(maxChunks int) Option {
	return func(fs *IRODS) {
		fs.MaxChunks = maxChunks
	}
}

func WithOpenFileAllowedPaths(openFileAllowedPaths []string) Option {
	return func(fs *IRODS) {
		fs.OpenFileAllowedPaths = openFileAllowedPaths
	}
}

// New returns a new IRODS file system, given a context, a zone, an iRODS client,
// and optional configuration options. The zone is used to provide the /<zone>
// top directory without having to query the iRODS server, and must match the
// zone configured in the iRODS client.
func New(ctx context.Context, zone string, client *iron.Client, options ...Option) *IRODS {
	fs := &IRODS{
		Context:   ctx,
		ChunkSize: DefaultChunkSize,
		MaxChunks: DefaultMaxChunks,
		Client:    client,
	}

	// Ensure zone is always set
	client.Zone = zone

	// From context
	if v, ok := ctx.Value(OpenFileAllowedPaths).([]string); ok {
		fs.OpenFileAllowedPaths = v
	}

	// From options
	for _, option := range options {
		option(fs)
	}

	return fs
}

func (fs *IRODS) Close() error {
	vfs.Logger(fs.Context).Debug("Closing iRODS connection")

	return fs.Client.Close()
}

// Username returns the username
func (fs *IRODS) Username() string {
	return fs.Client.Env().Username
}

// Groups returns the list of irods groups for the user
func (fs *IRODS) Groups() ([]string, error) {
	username := fs.Username()

	results := fs.Client.Query(msg.ICAT_COLUMN_COLL_USER_GROUP_NAME).Where(
		msg.ICAT_COLUMN_USER_NAME,
		fmt.Sprintf("= '%s'", username),
	).Execute(fs.Context)

	defer results.Close()

	out := []string{}

	for results.Next() {
		var s string

		if err := results.Scan(&s); err != nil {
			return nil, err
		}

		out = append(out, s)
	}

	if err := results.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

var (
	DefaultChunkSize = 32768 * 32 * 32 // 32MB
	DefaultMaxChunks = 2
)

func (fs *IRODS) FileRead(path string) (vfs.ReaderAt, error) {
	handle, err := fs.openFile(path, os.O_RDONLY)
	if err != nil {
		return handle, err
	}

	return &buffered.BufferedReaderAt{
		ReaderAt: &buffered.BackgroundReader{
			ReaderAt:  handle,
			ChunkSize: fs.ChunkSize,
		},
		ChunkSize: fs.ChunkSize,
		MaxChunks: fs.MaxChunks,
	}, nil
}

func (fs *IRODS) FileWrite(path string, flag int) (vfs.WriterAt, error) {
	handle, err := fs.openFile(path, flag)
	if err != nil {
		return handle, err
	}

	return &buffered.BufferedWriterAt{
		WriterAt: &buffered.BackgroundWriter{
			WriterAt:  handle,
			ChunkSize: fs.ChunkSize,
		},
		ChunkSize: fs.ChunkSize,
		MaxChunks: fs.MaxChunks,
	}, nil
}

func (fs *IRODS) OpenFile(path string, flag int, perm os.FileMode) (vfs.File, error) {
	// Check whether the OpenFile operation is enabled.
	if flag&os.O_RDWR != 0 {
		var supported bool

		for _, allowedPath := range fs.OpenFileAllowedPaths {
			if path == allowedPath || strings.HasPrefix(path, allowedPath+"/") {
				supported = true
			}
		}

		if !supported {
			return nil, vfs.ErrNotSupported
		}
	}

	handle, err := fs.openFile(path, flag)
	if err != nil {
		return handle, err
	}

	return &FileBufferedAt{
		file: handle,
		readerAt: &buffered.BufferedReaderAt{
			ReaderAt:  handle,
			ChunkSize: fs.ChunkSize,
			MaxChunks: fs.MaxChunks,
		},
		writerAt: &buffered.BufferedWriterAt{
			WriterAt:  handle,
			ChunkSize: fs.ChunkSize,
			MaxChunks: fs.MaxChunks,
		},
	}, nil
}

type FileBufferedAt struct {
	file     vfs.File
	readerAt *buffered.BufferedReaderAt
	writerAt *buffered.BufferedWriterAt
	offset   int64
	sync.Mutex
}

func (f *FileBufferedAt) Name() string {
	return f.file.Name()
}

func (f *FileBufferedAt) Readdir(int) ([]vfs.FileInfo, error) {
	return nil, syscall.ENOTDIR
}

func (f *FileBufferedAt) Stat() (vfs.FileInfo, error) {
	if err := f.writerAt.Flush(0, -1); err != nil {
		return nil, err
	}

	return f.file.Stat()
}

func (f *FileBufferedAt) Read(p []byte) (int, error) {
	f.Lock()
	defer f.Unlock()

	f.writerAt.Flush(f.offset, len(p))

	n, err := f.readerAt.ReadAt(p, f.offset)

	f.offset += int64(n)

	return n, err
}

func (f *FileBufferedAt) ReadAt(p []byte, off int64) (int, error) {
	f.Lock()
	defer f.Unlock()

	f.writerAt.Flush(off, len(p))

	return f.readerAt.ReadAt(p, off)
}

func (f *FileBufferedAt) Write(p []byte) (int, error) {
	f.Lock()
	defer f.Unlock()

	n, err := f.writerAt.WriteAt(p, f.offset)

	if n > 0 {
		f.readerAt.Invalidate(f.offset, n)
	}

	f.offset += int64(n)

	return n, err
}

func (f *FileBufferedAt) WriteAt(p []byte, off int64) (int, error) {
	f.Lock()
	defer f.Unlock()

	n, err := f.writerAt.WriteAt(p, off)

	if n > 0 {
		f.readerAt.Invalidate(off, n)
	}

	return n, err
}

func (f *FileBufferedAt) Seek(offset int64, whence int) (int64, error) {
	f.Lock()
	defer f.Unlock()

	if whence == io.SeekCurrent {
		offset += f.offset
	}

	if whence == io.SeekEnd {
		if err := f.writerAt.Flush(0, -1); err != nil {
			return 0, err
		}

		fi, err := f.Stat()
		if err != nil {
			return 0, err
		}

		offset += fi.Size()
	}

	f.offset = offset

	return f.offset, nil
}

func (f *FileBufferedAt) Truncate(size int64) error {
	f.Lock()
	defer f.Unlock()

	if err := f.writerAt.Flush(size, -1); err != nil {
		return err
	}

	f.readerAt.Invalidate(size, -1)

	return f.file.Truncate(size)
}

func (f *FileBufferedAt) Close() error {
	f.Lock()
	defer f.Unlock()

	return f.writerAt.Close()
}

var ForceRemove = false

func (fs *IRODS) Truncate(path string, size int64) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	for _, v := range fs.openFiles {
		if path == v.Name() {
			return v.Truncate(size)
		}
	}

	handle, err := fs.Client.OpenDataObject(fs.Context, path, os.O_WRONLY)
	if err != nil {
		return err
	}

	return multierr.Append(handle.Truncate(size), handle.Close())
}

func (fs *IRODS) Chown(path string, uid, gid int) error {
	return syscall.EPERM
}

func (fs *IRODS) Chmod(path string, mode os.FileMode) error {
	return nil // Silently accept
}

func (fs *IRODS) Chtimes(path string, atime, mtime time.Time) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	for _, v := range fs.openFiles {
		if path == v.Name() {
			return v.Chtimes(atime, mtime)
		}
	}

	handle, err := fs.Client.OpenDataObject(fs.Context, path, os.O_WRONLY)
	if err != nil {
		return err
	}

	defer handle.Close()

	return handle.Touch(mtime)
}

func (fs *IRODS) SetExtendedAttr(path, name string, value []byte) error {
	switch {
	case strings.HasPrefix(name, metaPrefixACL):
		username := strings.TrimPrefix(name, metaPrefixACL)

		return fs.Client.ModifyAccess(fs.Context, path, username, string(value), false)

	case name == metaInherit:
		return fs.Client.SetCollectionInheritance(fs.Context, path, true, false)

	case strings.HasPrefix(name, metaPrefix):
		var objectType api.ObjectType

		if _, err := fs.Client.GetCollection(fs.Context, path); err == nil {
			objectType = api.CollectionType
		} else if _, err = fs.Client.GetDataObject(fs.Context, path); err == nil {
			objectType = api.DataObjectType
		} else {
			return err
		}

		meta, err := fs.Client.ListMetadata(fs.Context, path, objectType)
		if err != nil {
			return err
		}

		name, i := findNumber(strings.TrimPrefix(name, metaPrefix))
		name, unit := findUnit(name)
		existing := findMeta(meta, name, unit, i)

		if existing != nil && existing.Value == string(value) {
			return nil
		}

		newMeta := []api.Metadata{{Name: name, Units: unit, Value: string(value)}}
		oldMeta := []api.Metadata{}

		if existing != nil {
			oldMeta = append(oldMeta, *existing)
		}

		return fs.Client.ModifyMetadata(fs.Context, path, objectType, newMeta, oldMeta)
	default:
		return nil
	}
}

func (fs *IRODS) UnsetExtendedAttr(path, name string) error {
	switch {
	case strings.HasPrefix(name, metaPrefixACL):
		username := strings.TrimPrefix(name, metaPrefixACL)

		return fs.Client.ModifyAccess(fs.Context, path, username, "null", false)

	case name == metaInherit:
		return fs.Client.SetCollectionInheritance(fs.Context, path, false, false)

	case strings.HasPrefix(name, metaPrefix):
		var objectType api.ObjectType

		if _, err := fs.Client.GetCollection(fs.Context, path); err == nil {
			objectType = api.CollectionType
		} else if _, err = fs.Client.GetDataObject(fs.Context, path); err == nil {
			objectType = api.DataObjectType
		} else {
			return err
		}

		meta, err := fs.Client.ListMetadata(fs.Context, path, objectType)
		if err != nil {
			return err
		}

		name, i := findNumber(strings.TrimPrefix(name, metaPrefix))
		name, unit := findUnit(name)
		existing := findMeta(meta, name, unit, i)

		if existing == nil {
			return nil
		}

		return fs.Client.RemoveMetadata(fs.Context, path, objectType, *existing)
	default:
		return nil
	}
}

func findMeta(meta []api.Metadata, name, unit string, i int) *api.Metadata {
	var j int

	for _, v := range meta {
		if v.Name == name && v.Units == unit {
			if i == j {
				return &v
			}

			j++
		}
	}

	return nil
}

func (fs *IRODS) SetExtendedAttrs(path string, attrs vfs.Attributes) error {
	// Refuse to delete all attributes
	if len(attrs) == 0 {
		return nil
	}

	var objectType api.ObjectType

	if _, err := fs.Client.GetCollection(fs.Context, path); err == nil {
		objectType = api.CollectionType
	} else if _, err = fs.Client.GetDataObject(fs.Context, path); err == nil {
		objectType = api.DataObjectType
	} else {
		return err
	}

	// Calculate the differences
	var (
		acl     []api.Access
		meta    []api.Metadata
		inherit bool
	)

	for name, value := range attrs {
		switch {
		case strings.HasPrefix(name, metaPrefixACL):
			username, zone := parseUser(strings.TrimPrefix(name, metaPrefixACL), fs.Client.Zone)

			acl = append(acl, api.Access{
				User: api.User{
					Name: username,
					Zone: zone,
				},
				Permission: string(value),
			})

		case name == metaInherit:
			inherit = true

		case strings.HasPrefix(name, metaPrefix):
			name, _ := findNumber(strings.TrimPrefix(name, metaPrefix))
			name, unit := findUnit(name)

			meta = append(meta, api.Metadata{Name: name, Units: unit, Value: string(value)})
		}
	}

	if err := fs.setACL(path, objectType, acl); err != nil {
		return err
	}

	if err := fs.setInherit(path, objectType, inherit); err != nil {
		return err
	}

	return fs.setMeta(path, objectType, meta)
}

func (fs *IRODS) setACL(path string, objectType api.ObjectType, acl []api.Access) error {
	if len(acl) == 0 {
		// Don't remove all acls
		return nil
	}

	current, err := fs.Client.ListAccess(fs.Context, path, objectType)
	if err != nil {
		return err
	}

	currentMap := toMap(current)
	myUsername := fs.Client.Env().Username + "#" + fs.Client.Env().Zone

	// Only set ACLs if we are currently owner of the path. This makes sure we do not alter anything in case the inherit bit is set.
	if val, ok := currentMap[myUsername]; (!ok || !val.Equal(Own)) && !fs.Client.Admin {
		vfs.Logger(fs.Context).Debugf("Skip ACL sync for %s because we are not the owner. Current ACL: %#v", path, current)

		return nil
	}

	aclMap := toMap(acl)

	// Add a none entry for all required users
	for oldUsername := range currentMap {
		if _, ok := aclMap[oldUsername]; !ok {
			aclMap[oldUsername] = Null
		}
	}

	// Save new permission for myUsername
	myPermission := aclMap[myUsername]

	delete(aclMap, myUsername)

	if myPermission.Level == 0 {
		myPermission = Null
	}

	// Set new permissions for all users
	for fusername, permission := range aclMap {
		if err := fs.Client.ModifyAccess(fs.Context, path, fusername, permission.Permission, false); err != nil {
			return err
		}
	}

	return fs.Client.ModifyAccess(fs.Context, path, myUsername, myPermission.Permission, false)
}

func toMap(acl []api.Access) map[string]Permission {
	m := make(map[string]Permission)

	for _, v := range acl {
		m[v.User.Name+"#"+v.User.Zone] = LookupPermission(v.Permission)
	}

	return m
}

func (fs *IRODS) setInherit(path string, objectType api.ObjectType, inherit bool) error {
	if objectType != api.CollectionType {
		return nil
	}

	return fs.Client.SetCollectionInheritance(fs.Context, path, inherit, false)
}

func (fs *IRODS) setMeta(path string, objectType api.ObjectType, meta []api.Metadata) error {
	curMeta, err := fs.Client.ListMetadata(fs.Context, path, objectType)
	if err != nil {
		return err
	}

	return fs.Client.ModifyMetadata(fs.Context, path, objectType, meta, curMeta)
}

func (fs *IRODS) Rename(oldpath, newpath string) error {
	// Is the path a collection?
	if _, err := fs.Client.GetCollection(fs.Context, oldpath); err == nil {
		// Rename should succeed if the target already exists and is an empty collection
		if _, err = fs.Client.GetCollection(fs.Context, newpath); err == nil {
			if err = fs.Client.DeleteCollection(fs.Context, newpath, true); err != nil {
				return err
			}
		}

		return fs.Client.RenameCollection(fs.Context, oldpath, newpath)
	} else if !errors.Is(err, api.ErrNoRowFound) {
		return err
	}

	// Is the path a data object?
	dataobject, err := fs.Client.GetDataObject(fs.Context, oldpath)

	// Wait until all replicas are closed
	for i := 2; err == nil && isLocked(dataobject) && i <= 10; i++ {
		vfs.Logger(fs.Context).Warnf("Client tries to rename locked file %s, stall %d seconds", dataobject.Path, i)

		time.Sleep(time.Duration(i) * time.Second)

		dataobject, err = fs.Client.GetDataObject(fs.Context, oldpath)
	}

	if err != nil {
		return err
	}

	if isLocked(dataobject) {
		return &msg.IRODSError{
			Code:    msg.HIERARCHY_ERROR,
			Message: fmt.Sprintf("tries to rename open file %s", dataobject.Path),
		}
	}

	// Rename should succeed if the target already exists and is a data object
	if _, err := fs.Client.GetDataObject(fs.Context, newpath); err == nil {
		if err = fs.Client.DeleteDataObject(fs.Context, newpath, true); err != nil {
			return err
		}
	}

	return fs.Client.RenameDataObject(fs.Context, oldpath, newpath)
}

func isLocked(obj *api.DataObject) bool {
	for _, replica := range obj.Replicas {
		if replica.Status != "1" {
			return true
		}
	}

	return false
}

func (fs *IRODS) Rmdir(path string) error {
	return fs.Client.DeleteCollection(fs.Context, path, ForceRemove)
}

func (fs *IRODS) Remove(path string) error {
	return fs.Client.DeleteDataObject(fs.Context, path, ForceRemove)
}

func (fs *IRODS) Mkdir(path string, _ os.FileMode) error {
	return fs.Client.CreateCollection(fs.Context, path)
}

func (fs *IRODS) List(path string) (vfs.ListerAt, error) {
	var (
		entries []vfs.FileInfo
		seen    bool
	)

	err := fs.Walk(path, func(path string, info vfs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !seen {
			seen = true

			return api.SkipSubDirs
		}

		entries = append(entries, info)

		return nil
	})

	return vfs.FileInfoListerAt(entries), notExistError(err)
}

func notExistError(err error) error {
	if errors.Is(err, api.ErrNoRowFound) {
		return os.ErrNotExist
	}

	return err
}

func (fs *IRODS) Walk(path string, fn vfs.WalkFunc) error {
	opts := []api.WalkOption{
		api.FetchAccess,
	}

	if vfs.Bool(fs.Context, vfs.ListWithXattrs) {
		opts = append(opts, api.FetchMetadata)
	}

	return notExistError(fs.Client.Walk(fs.Context, path, func(path string, record api.Record, err error) error {
		if err != nil {
			return fn(path, nil, err)
		}

		if record.IsDir() {
			coll, _ := record.Sys().(*api.Collection)

			return fn(path, fs.makeCollectionFileInfo(coll, record.Access(), record.Metadata()), nil)
		}

		obj, _ := record.Sys().(*api.DataObject)

		return fn(path, fs.makeDataObjectFileInfo(obj, record.Access(), record.Metadata()), nil)
	}, opts...))
}

var ErrUnexpectedEmptyXattrs = errors.New("unexpected empty xattrs")

func (fs *IRODS) Stat(path string) (vfs.FileInfo, error) {
	if path == "/" || path == "/"+fs.Client.Zone { // Avoid to create an irods connection just to stat the root
		return &fileInfo{
			name:    vfs.Base(path),
			modTime: time.Now(),
			mode:    os.FileMode(0o755) | os.ModeDir,
			permissionSet: &vfs.Permissions{
				Read:             true,
				GetExtendedAttrs: true,
			},
		}, nil
	}

	fs.lock.Lock()
	defer fs.lock.Unlock()

	for _, v := range fs.openFiles {
		if path == v.Name() {
			return v.Stat()
		}
	}

	return fs.stat(path)
}

func (fs *IRODS) Handle(path string) ([]byte, error) {
	if path == "/" {
		return []byte{0}, nil
	}

	if path == "/"+fs.Client.Zone {
		return []byte{1}, nil
	}

	fi, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	attrs, err := fi.Extended()
	if err != nil {
		return nil, err
	}

	id, ok := attrs.GetString("user.irods.global_id")
	if !ok {
		return nil, vfs.ErrNotSupported
	}

	parts := strings.SplitN(id, ":", 2)

	if len(parts) != 2 {
		return nil, vfs.ErrNotSupported
	}

	inode, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 8)

	binary.LittleEndian.PutUint64(buf, inode)

	return buf, nil
}

func (fs *IRODS) Path(handle []byte) (string, error) {
	if len(handle) == 1 { // Special handles
		if handle[0] == 0 {
			return "/", nil
		}

		return "/" + fs.Client.Zone, nil
	}

	if len(handle) != 8 {
		return "", os.ErrNotExist
	}

	inode := binary.LittleEndian.Uint64(handle)

	if inode == 0 {
		return "/", nil
	}

	result := fs.Client.Query(
		msg.ICAT_COLUMN_COLL_NAME,
		msg.ICAT_COLUMN_DATA_NAME,
	).Where(
		msg.ICAT_COLUMN_D_DATA_ID, fmt.Sprintf("= '%d'", inode),
	).Execute(fs.Context)

	defer result.Close()

	if result.Next() {
		var (
			coll string
			name string
		)

		if err := result.Scan(&coll, &name); err != nil {
			return "", err
		}

		if coll == "/" {
			return "/" + name, nil
		}

		return coll + "/" + name, nil
	}

	result = fs.Client.Query(
		msg.ICAT_COLUMN_COLL_NAME,
	).Where(
		msg.ICAT_COLUMN_COLL_ID, fmt.Sprintf("= '%d'", inode),
	).Execute(fs.Context)

	if result.Next() {
		var name string

		if err := result.Scan(&name); err != nil {
			return "", err
		}

		return name, nil
	}

	return "", os.ErrNotExist
}
