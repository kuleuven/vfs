package bytetree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/multierr"
)

// New creates a new ByteTree instance based on the given directory.
// The directory must be writeable as the ByteTree will write to it.
// The ByteTree will create two files in the given directory: "inodes.db"
// and "files.db".
func New(directory string) (*ByteTree, error) {
	inodes, err := os.OpenFile(filepath.Join(directory, "inodes.db"), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0o640)
	if err != nil {
		return nil, err
	}

	files, err := os.OpenFile(filepath.Join(directory, "files.db"), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0o640)
	if err != nil {
		inodes.Close()

		return nil, err
	}

	tree := &ByteTree{
		file: inodes,
		values: &values{
			file: files,
		},
	}

	return tree, nil
}

// ByteTree represents a tree based on []byte keys
// Starting from the root, each next node represents a next
// character in the handle. If there is only a single value
// stored with the prefix up to the current node, then it is
// a leaf node in the tree. If another value is added with
// the same prefix, additional child nodes are created until
// the prefixes differ.
type ByteTree struct {
	file   *os.File
	values *values
}

var ErrHasValue = errors.New("has value")

func (db *ByteTree) Put(value Value) error { //nolint:funlen,gocognit
	var offset int64

	for i := range len(value.Handle) {
		ptr, err := db.read(offset + int64(value.Handle[i])*8)
		if err != nil {
			return err
		}

		if ptr > 0 {
			offset = ptr

			continue
		}

		if ptr == 0 {
			ptr, err = db.values.Add(value)
			if err != nil {
				return err
			}

			return db.write(offset+int64(value.Handle[i])*8, 0, -ptr-1)
		}

		stored, err := db.values.Get(-ptr - 1)
		if err != nil {
			return err
		}

		if bytes.Equal(stored.Handle, value.Handle) {
			if stored.Path == value.Path {
				return nil
			}

			newptr, err := db.values.Add(value)
			if err != nil {
				return err
			}

			return db.write(offset+int64(value.Handle[i])*8, ptr, -newptr-1)
		}

		// Need to expand the tree
		newOffset, err := db.newOffset()
		if err != nil {
			return err
		}

		var next int64

		if len(stored.Handle) == i+1 {
			next = 256 * 8
		} else {
			next = int64(stored.Handle[i+1]) * 8
		}

		err = db.write(newOffset+next, 0, ptr)
		if err != nil {
			return err
		}

		err = db.write(offset+int64(value.Handle[i])*8, ptr, newOffset)
		if err != nil {
			return err
		}

		offset = newOffset
	}

	ptr, err := db.read(offset + 256*8)
	if err != nil {
		return err
	}

	if ptr < 0 {
		val, err := db.values.Get(-ptr - 1)
		if err != nil {
			return err
		}

		if val.Path == value.Path {
			return nil
		}
	}

	newptr, err := db.values.Add(value)
	if err != nil {
		return err
	}

	return db.write(offset+256*8, ptr, -newptr-1)
}

func (db *ByteTree) Get(handle []byte) (string, error) {
	var offset int64

	for i := range handle {
		ptr, err := db.read(offset + int64(handle[i])*8)
		if err != nil {
			return "", err
		}

		if ptr < 0 {
			val, err := db.values.Get(-ptr - 1)
			if err != nil {
				return "", err
			}

			if !bytes.Equal(val.Handle, handle) {
				return "", os.ErrNotExist
			}

			return val.Path, nil
		}

		if ptr == 0 {
			return "", os.ErrNotExist
		}

		offset = ptr
	}

	ptr, err := db.read(offset + 256*8)
	if err != nil {
		return "", err
	}

	if ptr == 0 {
		return "", os.ErrNotExist
	}

	val, err := db.values.Get(-ptr - 1)
	if err != nil {
		return "", err
	}

	return val.Path, nil
}

func (db *ByteTree) read(offset int64) (int64, error) {
	var buf [8]byte

	_, err := db.file.ReadAt(buf[:], offset)
	if errors.Is(err, io.EOF) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	return int64(binary.LittleEndian.Uint64(buf[:])), nil
}

func (db *ByteTree) write(offset, oldValue, newValue int64) error {
	lock, err := Lock(db.file)
	if err != nil {
		return err
	}

	defer lock.Unlock() //nolint:errcheck

	var buf [8]byte

	_, err = db.file.ReadAt(buf[:], offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	if binary.LittleEndian.Uint64(buf[:]) != uint64(oldValue) {
		return ErrHasValue
	}

	binary.LittleEndian.PutUint64(buf[:], uint64(newValue))

	_, err = db.file.WriteAt(buf[:], offset)

	return err
}

const chunkSize = 257 * 8

func (db *ByteTree) newOffset() (int64, error) {
	lock, err := Lock(db.file)
	if err != nil {
		return 0, err
	}

	defer lock.Unlock() //nolint:errcheck

	offset, err := db.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Make sure offset is a multiple of chunkSize
	// This is need when NewOffset() is called for the first time
	if offset%chunkSize != 0 || offset == 0 {
		offset += chunkSize - offset%chunkSize
	}

	_, err = db.file.WriteAt(make([]byte, chunkSize), offset)

	return offset, err
}

// Print the tree for debugging purposes
func (db *ByteTree) Print() {
	db.print(0, nil)
}

func (db *ByteTree) print(offset int64, prefix []byte) {
	for i := range 256 {
		ptr, err := db.read(offset + int64(i)*8)
		if err != nil {
			return
		}

		if ptr == 0 {
			continue
		}

		newPrefix := prefix
		newPrefix = append(newPrefix, byte(i))

		if ptr < 0 {
			val, err := db.values.Get(-ptr - 1)
			if err != nil {
				return
			}

			fmt.Printf("%x\t%x\t%s\n", newPrefix, val.Handle, val.Path)

			continue
		}

		if ptr < offset || ptr%chunkSize != 0 {
			fmt.Printf("bad pointer: %v\n", ptr)

			return
		}

		db.print(ptr, newPrefix)
	}

	ptr, err := db.read(offset + 256*8)
	if err != nil {
		return
	}

	if ptr < 0 {
		val, err := db.values.Get(-ptr - 1)
		if err != nil {
			return
		}

		fmt.Printf("%x\t%x\t%s\n", prefix, val.Handle, val.Path)
	}
}

func (db *ByteTree) Close() error {
	return multierr.Append(db.file.Close(), db.values.Close())
}
