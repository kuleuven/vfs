package irodsfs

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/kuleuven/iron/api"
	"github.com/kuleuven/vfs"
)

const metaPrefix = "user.meta."

const metaPrefixACL = "user.irods.acl."

const metaInherit = "user.irods.inherit"

func (fs *IRODS) Linearize(metadata []api.Metadata, access []api.Access) vfs.Attributes {
	return Linearize(metadata, access, fs.Client.Zone)
}

func Linearize(meta []api.Metadata, access []api.Access, defaultZone string) vfs.Attributes {
	seen := map[string]int{}
	result := vfs.Attributes{}

	for _, m := range meta {
		key := metaPrefix + m.Name

		if m.Units != "" || strings.HasSuffix(m.Name, "]") {
			key += "[" + escape(m.Units) + "]"
		} else if _, n := findNumber(m.Name); n > 0 {
			key += "[]"
		}

		n, ok := seen[key]

		seen[key] = n + 1

		if ok {
			key += "#" + strconv.Itoa(n+1)
		}

		result.Set(key, []byte(m.Value))
	}

	for _, a := range access {
		if a.User.Name == "" {
			continue // Skip unresolvable users
		}

		name := formatUser(a.User.Name, a.User.Zone, defaultZone)

		result.Set(metaPrefixACL+name, []byte(a.Permission))
	}

	return result
}

func (fs *IRODS) Delinearize(values vfs.Attributes) ([]api.Metadata, []api.Access) {
	return Delinearize(values, fs.Client.Zone)
}

func Delinearize(values vfs.Attributes, defaultZone string) ([]api.Metadata, []api.Access) {
	meta := []api.Metadata{}
	acl := []api.Access{}

	if values == nil {
		return meta, acl
	}

	for key, value := range values {
		if strings.HasPrefix(key, metaPrefixACL) {
			username, zone := parseUser(strings.TrimPrefix(key, metaPrefixACL), defaultZone)

			acl = append(acl, api.Access{
				User: api.User{
					Name: username,
					Zone: zone,
				},
				Permission: string(value),
			})

			continue
		}

		if !strings.HasPrefix(key, metaPrefix) {
			continue
		}

		key := strings.TrimPrefix(key, metaPrefix)

		key, _ = findNumber(key)

		key, unit := findUnit(key)

		meta = append(meta, api.Metadata{
			Name:  key,
			Units: unit,
			Value: string(value),
		})
	}

	return meta, acl
}

func escape(unit string) string {
	unit = strings.ReplaceAll(unit, "[", "\\[")
	unit = strings.ReplaceAll(unit, "]", "\\]")

	return unit
}

func unescape(unit string) string {
	unit = strings.ReplaceAll(unit, "\\[", "[")
	unit = strings.ReplaceAll(unit, "\\]", "]")

	return unit
}

var ReNumberSuffix = regexp.MustCompile(`#(\d+)$`)

func findNumber(name string) (string, int) {
	match := ReNumberSuffix.FindStringSubmatch(name)

	if len(match) == 0 {
		return name, 0
	}

	n, err := strconv.Atoi(match[1])
	if err != nil || n <= 1 {
		return name, 0
	}

	return strings.TrimSuffix(name, match[0]), n - 1
}

func findUnit(name string) (string, string) {
	if !strings.HasSuffix(name, "]") {
		return name, ""
	}

	name = strings.TrimSuffix(name, "]")

	parts := strings.Split(name, "[")
	if len(parts) < 2 {
		return name, ""
	}

	// Find last non-escaped part
	i := len(parts) - 1

	for i > 0 && parts[i-1][len(parts[i-1])-1] == '\\' {
		i--
	}

	key := strings.Join(parts[:i], "[")
	unit := strings.Join(parts[i:], "[")

	return key, unescape(unit)
}
