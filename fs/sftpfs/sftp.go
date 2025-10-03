package sftpfs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/io/readerat"
	"github.com/kuleuven/vfs/io/writerat"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
	"golang.org/x/crypto/ssh"
)

var _ vfs.AdvancedLinkFS = &SFTP{}

var MaxPacket = 32 * 1024 * 1024 // 32 MB

func New(conn *ssh.Client) (*SFTP, error) {
	sftpClient, err := sftp.NewClient(conn, sftp.UseConcurrentWrites(true), sftp.MaxPacketUnchecked(MaxPacket))
	if err != nil {
		return nil, err
	}

	return &SFTP{
		Client: sftpClient,
	}, nil
}

func NewPipe(r io.Reader, w io.WriteCloser) (*SFTP, error) {
	sftpClient, err := sftp.NewClientPipe(r, w, sftp.UseConcurrentWrites(true), sftp.MaxPacketUnchecked(MaxPacket))
	if err != nil {
		return nil, err
	}

	return &SFTP{
		Client: sftpClient,
	}, nil
}

type SFTP struct {
	Client *sftp.Client
}

func (s *SFTP) Chmod(path string, mode os.FileMode) error {
	return NormalizeError(s.Client.Chmod(path, mode))
}

func (s *SFTP) Chown(path string, uid, gid int) error {
	return NormalizeError(s.Client.Chown(path, uid, gid))
}

func (s *SFTP) Chtimes(path string, atime, mtime time.Time) error {
	return NormalizeError(s.Client.Chtimes(path, atime, mtime))
}

func (s *SFTP) FileRead(path string) (vfs.ReaderAt, error) {
	f, err := s.Client.OpenFile(path, os.O_RDONLY)

	return &struct {
		io.ReaderAt
		io.Closer
	}{
		ReaderAt: readerat.ReaderAt(f),
		Closer:   f,
	}, NormalizeError(err)
}

func (s *SFTP) FileWrite(path string, flags int) (vfs.WriterAt, error) {
	f, err := s.Client.OpenFile(path, flags)

	return &struct {
		io.WriterAt
		io.Closer
	}{
		WriterAt: writerat.WriterAt(f),
		Closer:   f,
	}, NormalizeError(err)
}

var ErrTypeAssertion = errors.New("type assertion failed")

func (s *SFTP) List(path string) (vfs.ListerAt, error) {
	entries, err := s.Client.ReadDir(path)
	if err != nil {
		return nil, NormalizeError(err)
	}

	enriched := make([]vfs.FileInfo, len(entries))

	for i, entry := range entries {
		enr, ok := entry.Sys().(*sftp.FileStat)
		if !ok {
			return nil, ErrTypeAssertion
		}

		enriched[i] = &SFTPFileInfo{
			FileInfo: entry,
			sys:      enr,
		}
	}

	return vfs.FileInfoListerAt(enriched), nil
}

func (s *SFTP) Link(target, path string) error {
	return NormalizeError(s.Client.Link(target, path))
}

func (s *SFTP) Symlink(target, path string) error {
	return NormalizeError(s.Client.Symlink(target, path))
}

func (s *SFTP) Mkdir(path string, perm os.FileMode) error {
	if err := s.Client.Mkdir(path); err != nil {
		return NormalizeError(err)
	}

	if err := s.Chmod(path, perm); err != nil {
		err = multierr.Append(err, s.Rmdir(path))

		return err
	}

	return nil
}

func (s *SFTP) Rmdir(path string) error {
	return NormalizeError(s.Client.RemoveDirectory(path))
}

func (s *SFTP) Remove(path string) error {
	return NormalizeError(s.Client.Remove(path))
}

func (s *SFTP) Rename(oldpath, newpath string) error {
	return NormalizeError(s.Client.Rename(oldpath, newpath))
}

func (s *SFTP) RealPath(path string) (string, error) {
	target, err := s.Client.RealPath(path)

	return target, NormalizeError(err)
}

func (s *SFTP) Stat(path string) (vfs.FileInfo, error) {
	stat, err := s.Client.Stat(path)
	if err != nil {
		return nil, NormalizeError(err)
	}

	sys, ok := stat.Sys().(*sftp.FileStat)
	if !ok {
		return nil, ErrTypeAssertion
	}

	return &SFTPFileInfo{
		FileInfo: stat,
		sys:      sys,
	}, nil
}

func (s *SFTP) Lstat(path string) (vfs.FileInfo, error) {
	stat, err := s.Client.Lstat(path)
	if err != nil {
		return nil, NormalizeError(err)
	}

	sys, ok := stat.Sys().(*sftp.FileStat)
	if !ok {
		return nil, ErrTypeAssertion
	}

	return &SFTPFileInfo{
		FileInfo: stat,
		sys:      sys,
	}, nil
}

func (s *SFTP) Truncate(path string, size int64) error {
	return NormalizeError(s.Client.Truncate(path, size))
}

func (s *SFTP) OpenFile(path string, flag int, perm os.FileMode) (vfs.File, error) {
	if flag&os.O_WRONLY == 0 && flag&os.O_RDWR == 0 { // Just read file
		f, err := s.Client.OpenFile(path, flag)

		return &SFTPFile{c: s, File: f}, NormalizeError(err)
	}

	f, err := s.Client.OpenFile(path, flag|os.O_EXCL)
	if isErrExist(err) && flag&os.O_EXCL == 0 {
		f, err = s.Client.OpenFile(path, flag&^os.O_CREATE)
		if err != nil {
			return nil, NormalizeError(err)
		}

		return &SFTPFile{c: s, File: f}, nil
	} else if err != nil {
		return nil, err
	}

	err = s.Chmod(path, perm)
	if err != nil {
		err = multierr.Append(err, f.Close())
		err = multierr.Append(err, s.Remove(path))

		return nil, err
	}

	return &SFTPFile{c: s, File: f}, err
}

func isErrExist(err error) bool {
	if os.IsExist(err) {
		return true
	}

	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "file exists")
}

func (s *SFTP) Open(path string) (vfs.File, error) {
	stat, statErr := s.Stat(path)
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, statErr
	}

	if stat != nil && stat.IsDir() {
		return &SFTPDirectory{s, stat, path, nil}, nil
	}

	f, err := s.Client.OpenFile(path, os.O_RDONLY)
	if err != nil {
		return nil, NormalizeError(err)
	}

	return &SFTPFile{s, f}, nil
}

func (s *SFTP) Readlink(path string) (string, error) {
	target, err := s.Client.ReadLink(path)

	return target, NormalizeError(err)
}

func (s *SFTP) NumLinks(path string) (uint64, error) {
	return 1, nil
}

func (s *SFTP) SetExtendedAttr(path, name string, value []byte) error {
	fi, err := s.Stat(path)
	if err != nil {
		return err
	}

	attrs, err := fi.Extended()
	if err != nil {
		return err
	}

	if currentValue, ok := attrs.Get(name); ok {
		if bytes.Equal(currentValue, value) {
			return nil
		}
	}

	attrs.Set(name, value)

	return s.SetExtendedAttrs(path, attrs)
}

func (s *SFTP) UnsetExtendedAttr(path, name string) error {
	fi, err := s.Stat(path)
	if err != nil {
		return err
	}

	attrs, err := fi.Extended()
	if err != nil {
		return err
	}

	if _, ok := attrs.Get(name); !ok {
		return nil
	}

	attrs.Delete(name)

	return s.SetExtendedAttrs(path, attrs)
}

func (s *SFTP) SetExtendedAttrs(path string, attrs vfs.Attributes) error {
	var sftpAttrs []sftp.StatExtended

	for attr, value := range attrs {
		sftpAttrs = append(sftpAttrs, sftp.StatExtended{
			ExtType: attr,
			ExtData: string(value),
		})
	}

	return NormalizeError(s.Client.SetExtendedData(path, sftpAttrs))
}

var LookupPrefix = "lookup"

var InodePrefix = "inode/"

func (s *SFTP) Handle(path string) ([]byte, error) {
	inodeStr, err := s.RealPath(LookupPrefix + path)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(inodeStr, InodePrefix) {
		return nil, vfs.ErrNotSupported
	}

	return hex.DecodeString(strings.TrimPrefix(inodeStr, InodePrefix))
}

func (s *SFTP) Path(handle []byte) (string, error) {
	return s.RealPath(InodePrefix + hex.EncodeToString(handle))
}

func (s *SFTP) Close() error {
	return s.Client.Close()
}

type SFTPFile struct {
	c *SFTP
	*sftp.File
}

func (f *SFTPFile) Stat() (vfs.FileInfo, error) {
	stat, err := f.File.Stat()
	if err != nil {
		return nil, NormalizeError(err)
	}

	sys, ok := stat.Sys().(*sftp.FileStat)
	if !ok {
		return nil, ErrTypeAssertion
	}

	return &SFTPFileInfo{
		FileInfo: stat,
		sys:      sys,
	}, nil
}

func (f *SFTPFile) Readdir(int) ([]vfs.FileInfo, error) {
	return nil, syscall.ENOTDIR
}

type SFTPDirectory struct {
	c       *SFTP
	stat    vfs.FileInfo
	name    string
	entries []vfs.FileInfo
}

func (d *SFTPDirectory) Name() string {
	return d.name
}

func (d *SFTPDirectory) Read([]byte) (int, error) {
	return 0, syscall.EISDIR
}

func (d *SFTPDirectory) Write([]byte) (int, error) {
	return 0, syscall.EISDIR
}

func (d *SFTPDirectory) WriteAt([]byte, int64) (int, error) {
	return 0, syscall.EISDIR
}

func (d *SFTPDirectory) ReadAt([]byte, int64) (int, error) {
	return 0, syscall.EISDIR
}

func (d *SFTPDirectory) Seek(int64, int) (int64, error) {
	return 0, syscall.EISDIR
}

func (d *SFTPDirectory) Truncate(int64) error {
	return syscall.EISDIR
}

func (d *SFTPDirectory) Readdir(n int) ([]vfs.FileInfo, error) {
	if d.entries == nil {
		entries, err := d.c.Client.ReadDir(d.name)
		if err != nil {
			return nil, NormalizeError(err)
		}

		d.entries = make([]vfs.FileInfo, len(entries))

		for i, entry := range entries {
			enr, ok := entry.Sys().(*sftp.FileStat)
			if !ok {
				return nil, ErrTypeAssertion
			}

			d.entries[i] = &SFTPFileInfo{
				FileInfo: entry,
				sys:      enr,
			}
		}
	}

	var err error

	if n < 0 {
		n = len(d.entries)
	} else if n >= len(d.entries) {
		n = len(d.entries)

		err = io.EOF
	}

	result := d.entries[:n]
	d.entries = d.entries[n:]

	return result, err
}

func (d *SFTPDirectory) Stat() (vfs.FileInfo, error) {
	return d.stat, nil
}

func (d *SFTPDirectory) Close() error {
	return nil
}

type SFTPFileInfo struct {
	os.FileInfo
	sys *sftp.FileStat
}

func (s *SFTPFileInfo) NumLinks() uint64 {
	// Bogus value...
	return 1
}

func (s *SFTPFileInfo) Extended() (vfs.Attributes, error) {
	attr := vfs.Attributes{}

	for _, e := range s.sys.Extended {
		if e.ExtType == "" {
			logrus.Warn("empty extended attribute type")

			continue
		}

		attr.Set(e.ExtType, []byte(e.ExtData))
	}

	return attr, nil
}

func (s *SFTPFileInfo) Sys() interface{} {
	return s.sys
}

func (s *SFTPFileInfo) Uid() uint32 { //nolint:staticcheck
	return s.sys.UID
}

func (s *SFTPFileInfo) Gid() uint32 { //nolint:staticcheck
	return s.sys.GID
}

func (s *SFTPFileInfo) Permissions() (*vfs.Permissions, error) {
	// Return bogus values, we cannot determine them
	return &vfs.Permissions{
		Read:             true,
		Write:            true,
		Delete:           true,
		Own:              true,
		GetExtendedAttrs: true,
		SetExtendedAttrs: true,
	}, nil
}
