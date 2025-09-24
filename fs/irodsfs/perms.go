package irodsfs

import (
	"slices"
)

var PermissionLevels = map[int][]string{
	1000: {"null", ""},
	1010: {"execute"},
	1020: {"read_annotation"},
	1030: {"read_system_metadata"},
	1040: {"read_metadata"},
	1050: {"read_object", "read object"},
	1060: {"write_annotation"},
	1070: {"create_metadata"},
	1080: {"modify_metadata"},
	1090: {"delete_metadata"},
	1100: {"administer_object"},
	1110: {"create_object"},
	1120: {"modify_object", "write_object", "write object"},
	1130: {"delete_object"},
	1140: {"create_token"},
	1150: {"delete_token"},
	1160: {"curate"},
	1200: {"own"},
}

type Permission struct {
	Level      int
	Permission string
}

func LookupPermission(name string) Permission {
	for level, perms := range PermissionLevels {
		if slices.Contains(perms, name) {
			return Permission{Level: level, Permission: perms[0]}
		}
	}

	return Permission{Level: 1000, Permission: "null"}
}

func (p Permission) String() string {
	return p.Permission
}

func (p Permission) Equal(other Permission) bool {
	return p.Level == other.Level
}

func (p Permission) Compare(other Permission) int {
	return p.Level - other.Level
}

func (p Permission) Includes(other Permission) bool {
	return p.Level >= other.Level
}

var (
	Own    = LookupPermission("own")
	Delete = LookupPermission("delete_object")
	Write  = LookupPermission("write_object")
	Read   = LookupPermission("read_object")
	Null   = LookupPermission("null")
)
