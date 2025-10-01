package vfs

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestBool(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() context.Context
		key      ContextKey
		expected bool
	}{
		{
			name: "true value",
			setup: func() context.Context {
				return context.WithValue(context.Background(), ListWithXattrs, true)
			},
			key:      ListWithXattrs,
			expected: true,
		},
		{
			name: "false value",
			setup: func() context.Context {
				return context.WithValue(context.Background(), ListWithXattrs, false)
			},
			key:      ListWithXattrs,
			expected: false,
		},
		{
			name:     "key not present",
			setup:    context.Background,
			key:      ListWithXattrs,
			expected: false,
		},
		{
			name: "wrong type - string",
			setup: func() context.Context {
				return context.WithValue(context.Background(), ListWithXattrs, "true")
			},
			key:      ListWithXattrs,
			expected: false,
		},
		{
			name: "wrong type - int",
			setup: func() context.Context {
				return context.WithValue(context.Background(), ListWithXattrs, 1)
			},
			key:      ListWithXattrs,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setup()

			result := Bool(ctx, tt.key)
			if result != tt.expected {
				t.Errorf("Bool() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() context.Context
		key      ContextKey
		expected string
	}{
		{
			name: "valid string",
			setup: func() context.Context {
				return context.WithValue(context.Background(), PersistentStorage, "/tmp/storage")
			},
			key:      PersistentStorage,
			expected: "/tmp/storage",
		},
		{
			name: "empty string",
			setup: func() context.Context {
				return context.WithValue(context.Background(), PersistentStorage, "")
			},
			key:      PersistentStorage,
			expected: "",
		},
		{
			name:     "key not present",
			setup:    context.Background,
			key:      PersistentStorage,
			expected: "",
		},
		{
			name: "wrong type - int",
			setup: func() context.Context {
				return context.WithValue(context.Background(), PersistentStorage, 42)
			},
			key:      PersistentStorage,
			expected: "",
		},
		{
			name: "wrong type - bool",
			setup: func() context.Context {
				return context.WithValue(context.Background(), PersistentStorage, true)
			},
			key:      PersistentStorage,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setup()

			result := String(ctx, tt.key)
			if result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestInt(t *testing.T) {
	testKey := ContextKey("test-int")

	tests := []struct {
		name     string
		setup    func() context.Context
		key      ContextKey
		expected int
	}{
		{
			name: "positive int",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, 42)
			},
			key:      testKey,
			expected: 42,
		},
		{
			name: "negative int",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, -10)
			},
			key:      testKey,
			expected: -10,
		},
		{
			name: "zero",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, 0)
			},
			key:      testKey,
			expected: 0,
		},
		{
			name:     "key not present",
			setup:    context.Background,
			key:      testKey,
			expected: 0,
		},
		{
			name: "wrong type - string",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, "42")
			},
			key:      testKey,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setup()

			result := Int(ctx, tt.key)
			if result != tt.expected {
				t.Errorf("Int() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestDuration(t *testing.T) {
	testKey := ContextKey("test-duration")

	tests := []struct {
		name     string
		setup    func() context.Context
		key      ContextKey
		expected time.Duration
	}{
		{
			name: "positive duration",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, 5*time.Millisecond)
			},
			key:      testKey,
			expected: 5 * time.Millisecond,
		},
		{
			name: "zero duration",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, time.Duration(0))
			},
			key:      testKey,
			expected: 0,
		},
		{
			name:     "key not present",
			setup:    context.Background,
			key:      testKey,
			expected: 0,
		},
		{
			name: "wrong type - int",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, 42)
			},
			key:      testKey,
			expected: 0,
		},
		{
			name: "wrong type - string",
			setup: func() context.Context {
				return context.WithValue(context.Background(), testKey, "5s")
			},
			key:      testKey,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setup()

			result := Duration(ctx, tt.key)
			if result != tt.expected {
				t.Errorf("Duration() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestAttributes_Get(t *testing.T) {
	tests := []struct {
		name      string
		attrs     Attributes
		key       string
		wantValue []byte
		wantOk    bool
	}{
		{
			name:      "existing key",
			attrs:     Attributes{"user.name": []byte("alice")},
			key:       "user.name",
			wantValue: []byte("alice"),
			wantOk:    true,
		},
		{
			name:      "non-existing key",
			attrs:     Attributes{"user.name": []byte("alice")},
			key:       "user.age",
			wantValue: nil,
			wantOk:    false,
		},
		{
			name:      "nil attributes",
			attrs:     nil,
			key:       "user.name",
			wantValue: nil,
			wantOk:    false,
		},
		{
			name:      "empty attributes",
			attrs:     Attributes{},
			key:       "user.name",
			wantValue: nil,
			wantOk:    false,
		},
		{
			name:      "empty value",
			attrs:     Attributes{"user.empty": []byte{}},
			key:       "user.empty",
			wantValue: []byte{},
			wantOk:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, ok := tt.attrs.Get(tt.key)
			if ok != tt.wantOk {
				t.Errorf("Get() ok = %v, want %v", ok, tt.wantOk)
			}

			if !bytes.Equal(value, tt.wantValue) {
				t.Errorf("Get() value = %v, want %v", value, tt.wantValue)
			}
		})
	}
}

func TestAttributes_GetString(t *testing.T) {
	tests := []struct {
		name      string
		attrs     Attributes
		key       string
		wantValue string
		wantOk    bool
	}{
		{
			name:      "existing key",
			attrs:     Attributes{"user.name": []byte("alice")},
			key:       "user.name",
			wantValue: "alice",
			wantOk:    true,
		},
		{
			name:      "non-existing key",
			attrs:     Attributes{"user.name": []byte("alice")},
			key:       "user.age",
			wantValue: "",
			wantOk:    false,
		},
		{
			name:      "nil attributes",
			attrs:     nil,
			key:       "user.name",
			wantValue: "",
			wantOk:    false,
		},
		{
			name:      "unicode string",
			attrs:     Attributes{"user.name": []byte("José")},
			key:       "user.name",
			wantValue: "José",
			wantOk:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, ok := tt.attrs.GetString(tt.key)
			if ok != tt.wantOk {
				t.Errorf("GetString() ok = %v, want %v", ok, tt.wantOk)
			}

			if value != tt.wantValue {
				t.Errorf("GetString() value = %q, want %q", value, tt.wantValue)
			}
		})
	}
}

func TestAttributes_Set(t *testing.T) {
	tests := []struct {
		name     string
		initial  Attributes
		key      string
		value    []byte
		wantSize int
	}{
		{
			name:     "set on existing",
			initial:  Attributes{"key1": []byte("val1")},
			key:      "key2",
			value:    []byte("val2"),
			wantSize: 2,
		},
		{
			name:     "overwrite existing",
			initial:  Attributes{"key1": []byte("val1")},
			key:      "key1",
			value:    []byte("newval"),
			wantSize: 1,
		},
		{
			name:     "set on empty",
			initial:  Attributes{},
			key:      "key1",
			value:    []byte("val1"),
			wantSize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Set(tt.key, tt.value)

			if len(tt.initial) != tt.wantSize {
				t.Errorf("Set() size = %d, want %d", len(tt.initial), tt.wantSize)
			}

			if val, ok := tt.initial.Get(tt.key); !ok || !bytes.Equal(val, tt.value) {
				t.Errorf("Set() value not set correctly")
			}
		})
	}
}

func TestAttributes_SetString(t *testing.T) {
	attrs := Attributes{}
	attrs.SetString("user.name", "alice")

	value, ok := attrs.GetString("user.name")
	if !ok {
		t.Error("SetString() value not found")
	}

	if value != "alice" {
		t.Errorf("SetString() value = %q, want %q", value, "alice")
	}
}

func TestAttributes_Delete(t *testing.T) {
	tests := []struct {
		name    string
		initial Attributes
		key     string
		wantLen int
	}{
		{
			name:    "delete existing",
			initial: Attributes{"key1": []byte("val1"), "key2": []byte("val2")},
			key:     "key1",
			wantLen: 1,
		},
		{
			name:    "delete non-existing",
			initial: Attributes{"key1": []byte("val1")},
			key:     "key2",
			wantLen: 1,
		},
		{
			name:    "delete from nil",
			initial: nil,
			key:     "key1",
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Delete(tt.key)

			if tt.initial != nil && len(tt.initial) != tt.wantLen {
				t.Errorf("Delete() length = %d, want %d", len(tt.initial), tt.wantLen)
			}

			if tt.initial != nil {
				if _, ok := tt.initial.Get(tt.key); ok {
					t.Error("Delete() key still exists")
				}
			}
		})
	}
}

func TestPermissions(t *testing.T) {
	// Test that Permissions struct can be created with various combinations
	tests := []struct {
		name  string
		perms Permissions
	}{
		{
			name: "all permissions",
			perms: Permissions{
				Read:             true,
				Write:            true,
				Delete:           true,
				Own:              true,
				GetExtendedAttrs: true,
				SetExtendedAttrs: true,
			},
		},
		{
			name: "read only",
			perms: Permissions{
				Read: true,
			},
		},
		{
			name:  "no permissions",
			perms: Permissions{},
		},
		{
			name: "write and delete",
			perms: Permissions{
				Write:  true,
				Delete: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the struct can be created and fields accessed
			_ = tt.perms.Read
			_ = tt.perms.Write
			_ = tt.perms.Delete
			_ = tt.perms.Own
			_ = tt.perms.GetExtendedAttrs
			_ = tt.perms.SetExtendedAttrs
		})
	}
}

func TestContextKeyType(t *testing.T) {
	// Verify ContextKey is a distinct type
	var key ContextKey = "test"
	if key != ContextKey("test") {
		t.Error("ContextKey type assertion failed")
	}

	// Verify it can be used as a context key
	ctx := context.WithValue(context.Background(), key, "value")
	if val := ctx.Value(key); val != "value" {
		t.Error("ContextKey cannot be used as context key")
	}
}
