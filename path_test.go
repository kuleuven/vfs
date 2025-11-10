package vfs

import (
	"testing"
)

func TestIsAbs(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"absolute path", "/home/user", true},
		{"root path", "/", true},
		{"relative path", "home/user", false},
		{"relative with dot", "./file", false},
		{"relative with double dot", "../file", false},
		{"empty string", "", false},
		{"just filename", "file.txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAbs(tt.path)
			if result != tt.expected {
				t.Errorf("IsAbs(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		name     string
		paths    []string
		expected string
	}{
		{"root and empty", []string{"/", ""}, "/"},
		{"simple join", []string{"home", "user", "file.txt"}, "home/user/file.txt"},
		{"root path", []string{"/", "home", "user"}, "/home/user"},
		{"with empty strings", []string{"home", "", "user"}, "home/user"},
		{"single element", []string{"file.txt"}, "file.txt"},
		{"multiple empty strings", []string{"home", "", "", "user"}, "home/user"},
		{"empty string at start", []string{"", "home", "user"}, "/home/user"},
		{"all non-empty", []string{"a", "b", "c"}, "a/b/c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Join(tt.paths...)
			if result != tt.expected {
				t.Errorf("Join(%v) = %q, want %q", tt.paths, result, tt.expected)
			}
		})
	}
}

func TestClean(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{"empty path", "", "."},
		{"root path", "/", "/"},
		{"simple path", "/home/user", "/home/user"},
		{"trailing slash", "/home/user/", "/home/user"},
		{"multiple trailing slashes", "/home/user///", "/home/user"},
		{"dot element", "/home/./user", "/home/user"},
		{"double dot element", "/home/user/../file", "/home/file"},
		{"multiple dots", "/home/user/../../file", "/file"},
		{"relative with dot", "./file", "file"},
		{"relative with double dot", "../file", "../file"},
		{"multiple double dots", "../../file", "../../file"},
		{"dot only", ".", "."},
		{"double dot only", "..", ".."},
		{"complex path", "/home/./user/../admin/./file", "/home/admin/file"},
		{"relative complex", "home/./user/../file", "home/file"},
		{"empty elements", "home//user///file", "home/user/file"},
		{"double dot at start", "../home/user", "../home/user"},
		{"double dot beyond root", "/../../file", "../../file"},
		{"relative all dots removed", "home/user/..", "home"},
		{"relative to current", "home/user/../..", "."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Clean(tt.path)
			if result != tt.expected {
				t.Errorf("Clean(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestDir(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{"simple path", "/home/user/file.txt", "/home/user"},
		{"nested path", "/home/user/docs/file.txt", "/home/user/docs"},
		{"one level deep", "/file.txt", "/"},
		{"relative path", "home/user/file.txt", "home/user"},
		{"relative one level", "home/file.txt", "home"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Dir(tt.path)
			if result != tt.expected {
				t.Errorf("Dir(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestDirInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"root path", "/"},
		{"no separator", "file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Dir(tt.path)
			if result != "" {
				t.Errorf("Dir(%q) = %q, want empty string for invalid input", tt.path, result)
			}
		})
	}
}

func TestIsPathSeparator(t *testing.T) {
	tests := []struct {
		name     string
		char     uint8
		expected bool
	}{
		{"forward slash", '/', true},
		{"backslash", '\\', false},
		{"letter a", 'a', false},
		{"space", ' ', false},
		{"dot", '.', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPathSeparator(tt.char)
			if result != tt.expected {
				t.Errorf("IsPathSeparator(%c) = %v, want %v", tt.char, result, tt.expected)
			}
		})
	}
}

func TestBase(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{"simple file", "/home/user/file.txt", "file.txt"},
		{"nested path", "/home/user/docs/readme.md", "readme.md"},
		{"directory", "/home/user/", "user"},
		{"root", "/", "."},
		{"empty string", "", "."},
		{"relative path", "home/user/file.txt", "file.txt"},
		{"just filename", "file.txt", "file.txt"},
		{"trailing slashes", "/home/user///", "user"},
		{"single level", "/file", "file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Base(tt.path)
			if result != tt.expected {
				t.Errorf("Base(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantDir  string
		wantFile string
	}{
		{"simple path", "/home/user/file.txt", "/home/user", "file.txt"},
		{"root file", "/file.txt", "/", "file.txt"},
		{"nested path", "/home/user/docs/readme.md", "/home/user/docs", "readme.md"},
		{"no separator", "file.txt", "", "file.txt"},
		{"relative path", "home/user/file.txt", "home/user", "file.txt"},
		{"relative single", "home/file.txt", "home", "file.txt"},
		{"directory only", "/home/user/", "/home/user", ""},
		{"root only", "/", "/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, file := Split(tt.path)
			if dir != tt.wantDir {
				t.Errorf("Split(%q) dir = %q, want %q", tt.path, dir, tt.wantDir)
			}

			if file != tt.wantFile {
				t.Errorf("Split(%q) file = %q, want %q", tt.path, file, tt.wantFile)
			}
		})
	}
}

func TestSeparatorConstant(t *testing.T) {
	if Separator != '/' {
		t.Errorf("Separator = %c, want '/'", Separator)
	}
}
