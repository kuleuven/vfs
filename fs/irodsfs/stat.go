package irodsfs

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/kuleuven/iron/api"
	"github.com/kuleuven/iron/msg"
	"github.com/kuleuven/vfs"
)

func (fs *IRODS) stat(path string) (vfs.FileInfo, error) {
	// Is the path a collection?
	collection, err := fs.Client.GetCollection(fs.Context, path)
	if errors.Is(err, api.ErrNoRowFound) {
		collection = nil
	} else if err != nil {
		return nil, err
	}

	if collection != nil && collection.ID > 0 {
		access, err := fs.Client.ListAccess(fs.Context, path, api.CollectionType)
		if err != nil {
			return nil, err
		}

		metadata, err := fs.Client.ListMetadata(fs.Context, path, api.CollectionType)
		if err != nil {
			return nil, err
		}

		return fs.makeCollectionFileInfo(collection, access, metadata), err
	}

	// Is the path a data object?
	dataobject, err := fs.Client.GetDataObject(fs.Context, path)
	if errors.Is(err, api.ErrNoRowFound) {
		return nil, syscall.ENOENT
	} else if err != nil {
		return nil, err
	}

	if dataobject == nil || dataobject.ID <= 0 {
		return nil, syscall.ENOENT
	}

	access, err := fs.Client.ListAccess(fs.Context, path, api.DataObjectType)
	if err != nil {
		return nil, err
	}

	metadata, err := fs.Client.ListMetadata(fs.Context, path, api.DataObjectType)
	if err != nil {
		return nil, err
	}

	return fs.makeDataObjectFileInfo(dataobject, access, metadata), err
}

func (fs *IRODS) makeCollectionFileInfo(collection *api.Collection, access []api.Access, metadata []api.Metadata) vfs.FileInfo {
	_, name := api.Split(collection.Path)

	attrs := fs.Linearize(metadata, access)

	attrs.SetString("user.irods.creator", collection.Owner)
	attrs.SetString("user.irods.global_id", fmt.Sprintf("%s:%d", fs.Client.Env().Zone, collection.ID))

	if collection.Inheritance {
		attrs.SetString(metaInherit, "")
	}

	return &fileInfo{
		name:          name,
		modTime:       collection.ModifiedAt,
		mode:          fs.getFileMode(collection.Owner, access, true),
		owner:         fs.ResolveUID(fs.Username()),
		group:         fs.ResolveUID(collection.Owner),
		extendedAttrs: attrs,
		permissionSet: permissionSet(fs.getPermission(access, fs.Username(), true)),
	}
}

func (fs *IRODS) makeDataObjectFileInfo(dataobject *api.DataObject, access []api.Access, metadata []api.Metadata) vfs.FileInfo {
	_, name := api.Split(dataobject.Path)

	attrs := fs.Linearize(metadata, access)

	attrs.SetString("user.irods.creator", dataobject.Replicas[0].Owner)
	attrs.SetString("user.irods.global_id", fmt.Sprintf("%s:%d", fs.Client.Env().Zone, dataobject.ID))

	return &fileInfo{
		name:          name,
		sizeInBytes:   dataobject.Replicas[0].Size,
		modTime:       dataobject.Replicas[0].ModifiedAt,
		mode:          fs.getFileMode(dataobject.Replicas[0].Owner, access, false),
		owner:         fs.ResolveUID(fs.Username()),
		group:         fs.ResolveUID(dataobject.Replicas[0].Owner),
		extendedAttrs: attrs,
		permissionSet: permissionSet(fs.getPermission(access, fs.Username(), true)),
	}
}

func (fs *IRODS) getFileMode(owner string, access []api.Access, isdir bool) os.FileMode {
	currentUserPerm := fs.getPermission(access, fs.Username(), true)
	ownerPerm := fs.getPermission(access, owner, true)
	publicPerm := fs.getPermission(access, "public", false)

	// user - group - public
	mode := permToOctal(currentUserPerm, isdir)<<6 | permToOctal(ownerPerm, isdir)<<3 | permToOctal(publicPerm, isdir)

	if isdir {
		mode |= os.ModeDir
	}

	return mode
}

func (fs *IRODS) getPermission(access []api.Access, username string, resolveGroups bool) Permission {
	names := []string{username}

	if resolveGroups {
		names = append(names, fs.ResolveGroups(username)...)
	}

	permission := Null

	for _, a := range access {
		if a.User.Zone != fs.Client.Zone || !slices.Contains(names, a.User.Name) {
			continue
		}

		if perm := LookupPermission(a.Permission); perm.Includes(permission) {
			permission = perm
		}
	}

	return permission
}

func permToOctal(perm Permission, isdir bool) os.FileMode {
	var execute os.FileMode

	if isdir {
		execute = 1
	}

	switch {
	case perm.Includes(Write):
		return 6 + execute
	case perm.Includes(Read):
		return 4 + execute
	default:
		return 0
	}
}

func (fs *IRODS) ResolveGroups(username string) []string {
	value, ok := fs.groupCache.Load(username)
	if ok {
		v, _ := value.([]string)

		return v
	}

	results := fs.Client.Query(msg.ICAT_COLUMN_COLL_USER_GROUP_NAME).Where(
		msg.ICAT_COLUMN_USER_NAME,
		fmt.Sprintf("= '%s'", username),
	).Where(
		msg.ICAT_COLUMN_USER_ZONE,
		fmt.Sprintf("= '%s'", fs.Client.Zone),
	).Execute(fs.Context)

	defer results.Close()

	out := []string{}

	for results.Next() {
		var s string

		if err := results.Scan(&s); err != nil {
			return []string{}
		}

		out = append(out, s)
	}

	if err := results.Err(); err != nil {
		return []string{}
	}

	fs.groupCache.Store(username, out)

	return out
}

func (fs *IRODS) ResolveUID(username string) int {
	value, ok := fs.uidCache.Load(username)
	if ok {
		v, _ := value.(int)

		return v
	}

	meta, err := fs.Client.ListMetadata(fs.Context, username, api.UserType, api.Equal(msg.ICAT_COLUMN_META_USER_ATTR_NAME, "dpa.uid_number"))
	if err != nil {
		fs.uidCache.Store(username, 0)

		return 0
	}

	if len(meta) == 0 {
		fs.uidCache.Store(username, 0)

		return 0
	}

	uid, err := strconv.Atoi(meta[0].Value)
	if err != nil {
		fs.uidCache.Store(username, 0)

		return 0
	}

	fs.uidCache.Store(username, uid)

	return uid
}

func (fs *IRODS) formatUser(user, zone string) string {
	if zone == "" || zone == fs.Client.Env().Zone {
		return user
	}

	return fmt.Sprintf("%s#%s", user, zone)
}

func (fs *IRODS) parseUser(user string) (string, string) {
	if strings.Contains(user, "#") {
		parts := strings.SplitN(user, "#", 2)

		return parts[0], parts[1]
	}

	return user, fs.Client.Env().Zone
}
