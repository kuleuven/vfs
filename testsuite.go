package vfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// RunTestSuiteRO runs comprehensive read-only tests for FS implementations
func RunTestSuiteRO(t *testing.T, fs FS) {
	t.Run("Stat", func(t *testing.T) {
		testStat(t, fs)
	})

	t.Run("List", func(t *testing.T) {
		testList(t, fs)
	})

	t.Run("Walk", func(t *testing.T) {
		testWalk(t, fs)
	})

	t.Run("FileRead", func(t *testing.T) {
		testFileReadExisting(t, fs)
	})

	t.Run("StatNonExistent", func(t *testing.T) {
		testStatNonExistent(t, fs)
	})

	t.Run("ListNonExistent", func(t *testing.T) {
		testListNonExistent(t, fs)
	})

	t.Run("FileReadNonExistent", func(t *testing.T) {
		testFileReadNonExistent(t, fs)
	})
}

// RunTestSuiteRW runs comprehensive read-write tests for FS implementations
func RunTestSuiteRW(t *testing.T, fs FS) {
	RunTestSuiteRO(t, fs)

	t.Run("FileOperations", func(t *testing.T) {
		testFileOperations(t, fs)
	})

	t.Run("DirectoryOperations", func(t *testing.T) {
		testDirectoryOperations(t, fs)
	})

	t.Run("AttributeOperations", func(t *testing.T) {
		testAttributeOperations(t, fs)
	})

	t.Run("PermissionOperations", func(t *testing.T) {
		testPermissionOperations(t, fs)
	})

	t.Run("TimeOperations", func(t *testing.T) {
		testTimeOperations(t, fs)
	})

	t.Run("RenameOperations", func(t *testing.T) {
		testRenameOperations(t, fs)
	})

	t.Run("TruncateOperations", func(t *testing.T) {
		testTruncateOperations(t, fs)
	})

	t.Run("ExtendedAttributes", func(t *testing.T) {
		testExtendedAttributes(t, fs)
	})
}

// RunTestSuiteAdvanced runs tests for advanced FS interfaces
func RunTestSuiteAdvanced(t *testing.T, fs FS) {
	RunTestSuiteRW(t, fs)

	if hfs, ok := fs.(HandleFS); ok {
		t.Run("HandleFS", func(t *testing.T) {
			testHandleFS(t, hfs)
		})
	}

	if hrfs, ok := fs.(HandleResolveFS); ok {
		t.Run("HandleResolveFS", func(t *testing.T) {
			testHandleResolveFS(t, hrfs)
		})
	}

	if offs, ok := fs.(OpenFileFS); ok {
		t.Run("OpenFileFS", func(t *testing.T) {
			testOpenFileFS(t, offs)
		})
	}

	if sfs, ok := fs.(SymlinkFS); ok {
		t.Run("SymlinkFS", func(t *testing.T) {
			testSymlinkFS(t, sfs)
		})
	}

	if lfs, ok := fs.(LinkFS); ok {
		t.Run("LinkFS", func(t *testing.T) {
			testLinkFS(t, lfs)
		})
	}

	if wfs, ok := fs.(WalkFS); ok {
		t.Run("WalkFS", func(t *testing.T) {
			testWalkFS(t, wfs)
		})
	}

	if seafs, ok := fs.(SetExtendedAttrsFS); ok {
		t.Run("SetExtendedAttrsFS", func(t *testing.T) {
			testSetExtendedAttrsFS(t, seafs)
		})
	}
}

// Individual test functions

func testStat(t *testing.T, fs FS) {
	finfo, err := fs.Stat("/")
	if err != nil {
		t.Fatal(err)
	}

	if finfo.Name() == "" {
		t.Error("Root name should not be empty")
	}

	if !finfo.IsDir() {
		t.Error("Root should be a directory")
	}

	t.Logf("Root info: %s (size: %d, mode: %v)", finfo.Name(), finfo.Size(), finfo.Mode())

	// Test extended attributes if available
	if attrs, err := finfo.Extended(); err == nil {
		t.Logf("Extended attributes available: %d attrs", len(attrs))
	}

	// Test permissions if available
	if perms, err := finfo.Permissions(); err == nil {
		t.Logf("Permissions: read=%v, write=%v, delete=%v, own=%v",
			perms.Read, perms.Write, perms.Delete, perms.Own)
	}
}

func testStatNonExistent(t *testing.T, fs FS) {
	_, err := fs.Stat("/nonexistent/path/that/should/not/exist")
	if err == nil {
		t.Error("Expected error for non-existent path")
	}
}

func testList(t *testing.T, fs FS) {
	lister, err := fs.List("/")
	if err != nil {
		t.Fatal(err)
	}
	defer lister.Close()

	buf := make([]FileInfo, 10)
	n, err := lister.ListAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	t.Logf("Found %d entries in root directory", n)
	for i, finfo := range buf[:n] {
		t.Logf("  [%d] %s (dir: %v, size: %d)", i, finfo.Name(), finfo.IsDir(), finfo.Size())
	}

	// Test listing with different buffer sizes
	if n > 0 {
		smallBuf := make([]FileInfo, 1)
		count := 0
		offset := int64(0)

		for {
			m, err := lister.ListAt(smallBuf, offset)
			if m == 0 {
				break
			}
			count += m
			offset += int64(m)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		if count != n {
			t.Errorf("Expected %d entries with small buffer, got %d", n, count)
		}
	}
}

func testListNonExistent(t *testing.T, fs FS) {
	_, err := fs.List("/nonexistent/directory")
	if err == nil {
		t.Error("Expected error for non-existent directory")
	}
}

func testWalk(t *testing.T, fs FS) {
	var paths []string
	err := Walk(fs, "/", func(path string, info FileInfo, err error) error {
		if err != nil {
			t.Logf("Walk error at %s: %v", path, err)
			return err
		}
		paths = append(paths, path)
		t.Logf("Walk: %s (dir: %v)", path, info.IsDir())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(paths) == 0 {
		t.Error("Walk should find at least the root directory")
	}
}

func testFileReadExisting(t *testing.T, fs FS) {
	// This test assumes there's at least one readable file in the filesystem
	// In a real test, you might want to create a file first or skip if no files exist
	lister, err := fs.List("/")
	if err != nil {
		t.Skip("Cannot list root directory")
	}
	defer lister.Close()

	buf := make([]FileInfo, 100)
	n, _ := lister.ListAt(buf, 0)

	for _, finfo := range buf[:n] {
		if !finfo.IsDir() {
			fr, err := fs.FileRead("/" + finfo.Name())
			if err == nil {
				defer fr.Close()
				readBuf := make([]byte, 1024)
				readN, _ := fr.ReadAt(readBuf, 0)
				t.Logf("Read %d bytes from %s", readN, finfo.Name())
				return
			}
		}
	}

	t.Skip("No readable files found for testing")
}

func testFileReadNonExistent(t *testing.T, fs FS) {
	_, err := fs.FileRead("/nonexistent/file.txt")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func testFileOperations(t *testing.T, fs FS) {
	testPath := "/test_file_operations.txt"
	testContent := "Hello, VFS World! This is a test file."

	// Create and write
	fw, err := fs.FileWrite(testPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		t.Fatal(err)
	}

	n, err := fw.WriteAt([]byte(testContent), 0)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(testContent) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testContent), n)
	}

	err = fw.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Verify file exists and has correct size
	finfo, err := fs.Stat(testPath)
	if err != nil {
		t.Fatal(err)
	}

	if finfo.Size() != int64(len(testContent)) {
		t.Errorf("Expected file size %d, got %d", len(testContent), finfo.Size())
	}

	// Read back
	fr, err := fs.FileRead(testPath)
	if err != nil {
		t.Fatal(err)
	}

	readBuf := make([]byte, len(testContent)+10)
	readN, err := fr.ReadAt(readBuf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	if readN != len(testContent) {
		t.Errorf("Expected to read %d bytes, read %d", len(testContent), readN)
	}

	if string(readBuf[:readN]) != testContent {
		t.Errorf("Expected content '%s', got '%s'", testContent, string(readBuf[:readN]))
	}

	err = fr.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Test append
	fw2, err := fs.FileWrite(testPath, os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}

	appendContent := " Appended text."
	_, err = fw2.WriteAt([]byte(appendContent), int64(len(testContent)))
	if err != nil {
		t.Fatal(err)
	}

	err = fw2.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Clean up
	err = fs.Remove(testPath)
	if err != nil {
		t.Fatal(err)
	}
}

func testDirectoryOperations(t *testing.T, fs FS) {
	testDir := "/test_directory"

	// Create directory
	err := fs.Mkdir(testDir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// Verify directory exists
	finfo, err := fs.Stat(testDir)
	if err != nil {
		t.Fatal(err)
	}

	if !finfo.IsDir() {
		t.Error("Created path should be a directory")
	}

	// Test nested directory creation
	nestedDir := testDir + "/nested/deep"

	err = MkdirAll(fs, nestedDir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file in the directory
	testFile := testDir + "/test_file.txt"
	fw, err := fs.FileWrite(testFile, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte("test"), 0)
	fw.Close()

	// List directory contents
	lister, err := fs.List(testDir)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]FileInfo, 10)
	n, _ := lister.ListAt(buf, 0)
	lister.Close()

	found := false
	for _, finfo := range buf[:n] {
		t.Logf("Directory content: %s", finfo.Name())
		if finfo.Name() == "test_file.txt" {
			found = true
		}
	}

	if !found {
		t.Error("Created file not found in directory listing")
	}

	// Clean up
	fs.Remove(testFile)
	fs.Rmdir(testDir)
}

func testAttributeOperations(t *testing.T, fs FS) {
	testFile := "/test_attributes.txt"

	// Create test file
	fw, err := fs.FileWrite(testFile, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte("test"), 0)
	fw.Close()

	// Test getting initial file info
	finfo, err := fs.Stat(testFile)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Initial file info: uid=%d, gid=%d, links=%d",
		finfo.Uid(), finfo.Gid(), finfo.NumLinks())

	// Clean up
	fs.Remove(testFile)
}

func testPermissionOperations(t *testing.T, fs FS) {
	testFile := "/test_permissions.txt"

	// Create test file
	fw, err := fs.FileWrite(testFile, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte("test"), 0)
	fw.Close()

	// Test chmod
	err = fs.Chmod(testFile, 0644)
	if err != nil {
		t.Logf("Chmod not supported or failed: %v", err)
	}

	// Test chown (might not be supported in many implementations)
	err = fs.Chown(testFile, 1000, 1000)
	if err != nil {
		t.Logf("Chown not supported or failed: %v", err)
	}

	// Clean up
	fs.Remove(testFile)
}

func testTimeOperations(t *testing.T, fs FS) {
	testFile := "/test_times.txt"

	// Create test file
	fw, err := fs.FileWrite(testFile, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte("test"), 0)
	fw.Close()

	// Test chtimes
	newTime := time.Now().Add(-24 * time.Hour)
	err = fs.Chtimes(testFile, newTime, newTime)
	if err != nil {
		t.Logf("Chtimes not supported or failed: %v", err)
	} else {
		// Verify the time was set
		finfo, err := fs.Stat(testFile)
		if err == nil {
			if !finfo.ModTime().Equal(newTime) {
				// Allow for some precision loss
				diff := finfo.ModTime().Sub(newTime)
				if diff > time.Second && diff < -time.Second {
					t.Errorf("Expected mtime %v, got %v", newTime, finfo.ModTime())
				}
			}
		}
	}

	// Clean up
	fs.Remove(testFile)
}

func testRenameOperations(t *testing.T, fs FS) {
	oldPath := "/test_rename_old.txt"
	newPath := "/test_rename_new.txt"
	testContent := "rename test content"

	// Create test file
	fw, err := fs.FileWrite(oldPath, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte(testContent), 0)
	fw.Close()

	// Rename file
	err = fs.Rename(oldPath, newPath)
	if err != nil {
		t.Fatal(err)
	}

	// Verify old path doesn't exist
	_, err = fs.Stat(oldPath)
	if err == nil {
		t.Error("Old path should not exist after rename")
	}

	// Verify new path exists and has correct content
	finfo, err := fs.Stat(newPath)
	if err != nil {
		t.Fatal(err)
	}

	if finfo.Size() != int64(len(testContent)) {
		t.Errorf("Renamed file has wrong size: expected %d, got %d",
			len(testContent), finfo.Size())
	}

	// Read content to verify
	fr, err := fs.FileRead(newPath)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(testContent))
	n, _ := fr.ReadAt(buf, 0)
	fr.Close()

	if string(buf[:n]) != testContent {
		t.Errorf("Renamed file has wrong content: expected '%s', got '%s'",
			testContent, string(buf[:n]))
	}

	// Clean up
	fs.Remove(newPath)
}

func testTruncateOperations(t *testing.T, fs FS) {
	testFile := "/test_truncate.txt"
	originalContent := "This is a long test content that will be truncated"

	// Create test file
	fw, err := fs.FileWrite(testFile, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte(originalContent), 0)
	fw.Close()

	// Truncate to smaller size
	newSize := int64(10)
	err = fs.Truncate(testFile, newSize)
	if err != nil {
		t.Fatal(err)
	}

	// Verify new size
	finfo, err := fs.Stat(testFile)
	if err != nil {
		t.Fatal(err)
	}

	if finfo.Size() != newSize {
		t.Errorf("Expected truncated size %d, got %d", newSize, finfo.Size())
	}

	// Verify content
	fr, err := fs.FileRead(testFile)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, newSize)
	n, _ := fr.ReadAt(buf, 0)
	fr.Close()

	expectedContent := originalContent[:newSize]
	if string(buf[:n]) != expectedContent {
		t.Errorf("Expected truncated content '%s', got '%s'",
			expectedContent, string(buf[:n]))
	}

	// Clean up
	fs.Remove(testFile)
}

func testExtendedAttributes(t *testing.T, fs FS) {
	testFile := "/test_xattrs.txt"
	attrName := "user.test"
	attrValue := []byte("test value")

	// Create test file
	fw, err := fs.FileWrite(testFile, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte("test"), 0)
	fw.Close()

	// Set extended attribute
	err = fs.SetExtendedAttr(testFile, attrName, attrValue)
	if err != nil {
		t.Logf("SetExtendedAttr not supported or failed: %v", err)
	} else {
		// Try to read back the attribute
		finfo, err := fs.Stat(testFile)
		if err == nil {
			if attrs, err := finfo.Extended(); err == nil {
				if value, ok := attrs.Get(attrName); ok {
					if string(value) != string(attrValue) {
						t.Errorf("Expected xattr value '%s', got '%s'",
							string(attrValue), string(value))
					}
				} else {
					t.Error("Extended attribute not found after setting")
				}
			}
		}

		// Unset extended attribute
		err = fs.UnsetExtendedAttr(testFile, attrName)
		if err != nil {
			t.Logf("UnsetExtendedAttr failed: %v", err)
		}
	}

	// Clean up
	fs.Remove(testFile)
}

// Tests for advanced interfaces

func testHandleFS(t *testing.T, hfs HandleFS) {
	handle, err := hfs.Handle("/")
	if err != nil {
		t.Fatal(err)
	}

	if len(handle) == 0 {
		t.Error("Handle should not be empty")
	}

	t.Logf("Root handle: %x", handle)
}

func testHandleResolveFS(t *testing.T, hrfs HandleResolveFS) {
	handle, err := hrfs.Handle("/")
	if err != nil {
		t.Fatal(err)
	}

	path, err := hrfs.Path(handle)
	if err != nil {
		t.Fatal(err)
	}

	if path != "/" {
		t.Errorf("Expected path '/', got '%s'", path)
	}
}

func testOpenFileFS(t *testing.T, offs OpenFileFS) {
	testFile := "/test_openfile.txt"
	testContent := "OpenFile test content"

	// Create and write using OpenFile
	file, err := offs.OpenFile(testFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}

	n, err := file.Write([]byte(testContent))
	if err != nil {
		t.Fatal(err)
	}

	if n != len(testContent) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testContent), n)
	}

	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read back using OpenFile
	file2, err := offs.OpenFile(testFile, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(testContent))
	readN, err := file2.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	if string(buf[:readN]) != testContent {
		t.Errorf("Expected content '%s', got '%s'", testContent, string(buf[:readN]))
	}

	file2.Close()

	// Clean up
	offs.Remove(testFile)
}

func testSymlinkFS(t *testing.T, sfs SymlinkFS) {
	target := "/test_symlink_target.txt"
	link := "/test_symlink.txt"

	// Create target file
	fw, err := sfs.FileWrite(target, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte("target content"), 0)
	fw.Close()

	// Create symlink
	err = sfs.Symlink(target, link)
	if err != nil {
		t.Fatal(err)
	}

	// Read symlink
	linkTarget, err := sfs.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}

	if linkTarget != target {
		t.Errorf("Expected symlink target '%s', got '%s'", target, linkTarget)
	}

	// Test lstat vs stat
	linkInfo, err := sfs.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}

	targetInfo, err := sfs.Stat(link)
	if err != nil {
		t.Fatal(err)
	}

	// linkInfo should be the symlink, targetInfo should be the target
	if linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Error("Lstat should return symlink mode")
	}

	if targetInfo.Mode()&os.ModeSymlink != 0 {
		t.Error("Stat should return target mode, not symlink mode")
	}

	// Clean up
	sfs.Remove(link)
	sfs.Remove(target)
}

func testLinkFS(t *testing.T, lfs LinkFS) {
	original := "/test_link_original.txt"
	hardlink := "/test_hardlink.txt"
	testContent := "hard link test content"

	// Create original file
	fw, err := lfs.FileWrite(original, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte(testContent), 0)
	fw.Close()

	// Create hard link
	err = lfs.Link(original, hardlink)
	if err != nil {
		t.Fatal(err)
	}

	// Both files should have the same content
	fr1, err := lfs.FileRead(original)
	if err != nil {
		t.Fatal(err)
	}

	fr2, err := lfs.FileRead(hardlink)
	if err != nil {
		t.Fatal(err)
	}

	buf1 := make([]byte, len(testContent))
	buf2 := make([]byte, len(testContent))

	n1, _ := fr1.ReadAt(buf1, 0)
	n2, _ := fr2.ReadAt(buf2, 0)

	fr1.Close()
	fr2.Close()

	if string(buf1[:n1]) != string(buf2[:n2]) {
		t.Error("Hard linked files should have identical content")
	}

	// Check link count
	finfo, err := lfs.Stat(original)
	if err == nil {
		if finfo.NumLinks() < 2 {
			t.Errorf("Expected at least 2 links, got %d", finfo.NumLinks())
		}
	}

	// Clean up
	lfs.Remove(hardlink)
	lfs.Remove(original)
}

func testWalkFS(t *testing.T, wfs WalkFS) {
	var paths []string
	err := wfs.Walk("/", func(path string, info FileInfo, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, path)
		t.Logf("WalkFS: %s (dir: %v)", path, info.IsDir())
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(paths) == 0 {
		t.Error("WalkFS should find at least the root directory")
	}

	// Test walk with early termination
	count := 0
	maxCount := 3
	err = wfs.Walk("/", func(path string, info FileInfo, err error) error {
		if err != nil {
			return err
		}
		count++
		if count >= maxCount {
			return filepath.SkipDir
		}
		return nil
	})

	// Error might be SkipDir, which is expected
	if err != nil && err != filepath.SkipDir {
		t.Fatal(err)
	}
}

func testSetExtendedAttrsFS(t *testing.T, seafs SetExtendedAttrsFS) {
	testFile := "/test_set_xattrs.txt"

	// Create test file
	fw, err := seafs.FileWrite(testFile, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	fw.WriteAt([]byte("test"), 0)
	fw.Close()

	// Set multiple extended attributes at once
	attrs := Attributes{
		"user.test1": []byte("value1"),
		"user.test2": []byte("value2"),
		"user.test3": []byte("value3"),
	}

	err = seafs.SetExtendedAttrs(testFile, attrs)
	if err != nil {
		t.Logf("SetExtendedAttrs not supported or failed: %v", err)
	} else {
		// Verify attributes were set
		finfo, err := seafs.Stat(testFile)
		if err == nil {
			if readAttrs, err := finfo.Extended(); err == nil {
				for name, expectedValue := range attrs {
					if actualValue, ok := readAttrs.Get(name); ok {
						if string(actualValue) != string(expectedValue) {
							t.Errorf("Attribute %s: expected '%s', got '%s'",
								name, string(expectedValue), string(actualValue))
						}
					} else {
						t.Errorf("Attribute %s not found after batch set", name)
					}
				}
			}
		}
	}

	// Clean up
	seafs.Remove(testFile)
}
