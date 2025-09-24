package irodsfs

import (
	"reflect"
	"slices"
	"testing"
)

func TestPerms(t *testing.T) {
	toSort := []Permission{
		Own, Write, Read, Null,
	}

	slices.SortFunc(toSort, func(a, b Permission) int {
		return a.Compare(b)
	})

	if !reflect.DeepEqual(toSort, []Permission{Null, Read, Write, Own}) {
		t.Errorf("Expected %#v, got %#v", []Permission{Null, Read, Write, Own}, toSort)
	}
}
