package sftpfs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/kuleuven/vfs"
	"github.com/kuleuven/vfs/io/readerat"
	"github.com/kuleuven/vfs/io/writerat"
	"github.com/pkg/sftp"
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
	*sftp.Client
}

func (s *SFTP) FileRead(path string) (vfs.ReaderAt, error) {
	f, err := s.Client.OpenFile(path, os.O_RDONLY)

	return &struct {
		io.ReaderAt
		io.Closer
	}{
		ReaderAt: readerat.ReaderAt(f),
		Closer:   f,
	}, err
}

func (s *SFTP) FileWrite(path string, flags int) (vfs.WriterAt, error) {
	f, err := s.Client.OpenFile(path, flags)

	return &struct {
		io.WriterAt
		io.Closer
	}{
		WriterAt: writerat.WriterAt(f),
		Closer:   f,
	}, err
}

var ErrTypeAssertion = errors.New("type assertion failed")

func (s *SFTP) List(path string) (vfs.ListerAt, error) {
	entries, err := s.ReadDir(path)
	if err != nil {
		return nil, err
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

func (s *SFTP) Mkdir(path string, perm os.FileMode) error {
	if err := s.Client.Mkdir(path); err != nil {
		return err
	}

	if err := s.Chmod(path, perm); err != nil {
		err = multierr.Append(err, s.RemoveDirectory(path))

		return err
	}

	return nil
}

func (s *SFTP) Rmdir(path string) error {
	return s.RemoveDirectory(path)
}

func (s *SFTP) Stat(path string) (vfs.FileInfo, error) {
	stat, err := s.Client.Stat(path)
	if err != nil {
		return nil, err
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
		return nil, err
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

func (s *SFTP) OpenFile(path string, flag int, perm os.FileMode) (vfs.File, error) {
	if flag&os.O_WRONLY == 0 && flag&os.O_RDWR == 0 { // Just read file
		f, err := s.Client.OpenFile(path, flag)

		return &SFTPFile{c: s, File: f}, err
	}

	f, err := s.Client.OpenFile(path, flag|os.O_EXCL)
	if isErrExist(err) && flag&os.O_EXCL == 0 {
		f, err = s.Client.OpenFile(path, flag&^os.O_CREATE)
		if err != nil {
			return nil, err
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
		return &SFTPDirectory{s, stat, path}, nil
	}

	f, err := s.Client.OpenFile(path, os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	return &SFTPFile{s, f}, nil
}

func (s *SFTP) Readlink(path string) (string, error) {
	return s.ReadLink(path)
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
	sftpAttrs := make([]sftp.StatExtended, 0, len(attrs))

	for attr, value := range attrs {
		sftpAttrs = append(sftpAttrs, sftp.StatExtended{
			ExtType: attr,
			ExtData: string(value),
		})
	}

	return s.SetExtendedData(path, sftpAttrs)
}

var LookupPrefix = "lookup"

var LookupEtagPrefix = "lookup-etag"

var InodePrefix = "inode/"

var EtagPrefix = "etag/"

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

func (s *SFTP) Etag(path string) (string, error) {
	etagStr, err := s.RealPath(LookupEtagPrefix + path)
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(etagStr, EtagPrefix) {
		return "", vfs.ErrNotSupported
	}

	return strings.TrimPrefix(etagStr, EtagPrefix), nil
}

type SFTPFile struct {
	c *SFTP
	*sftp.File
}

func (f *SFTPFile) Stat() (vfs.FileInfo, error) {
	stat, err := f.File.Stat()
	if err != nil {
		return nil, err
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
	c    *SFTP
	stat vfs.FileInfo
	name string
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
	entries, err := d.c.ReadDir(d.name)
	if err != nil {
		return nil, err
	}

	if len(entries) > n && n > 0 {
		entries = entries[:n]
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

	return enriched, err
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
	return 1
}

func (s *SFTPFileInfo) Extended() (vfs.Attributes, error) {
	attr := vfs.Attributes{}

	for _, e := range s.sys.Extended {
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
	// TODO: add way to resolve them similar to Handle/Path functions
	return &vfs.Permissions{
		Read:             true,
		Write:            true,
		Delete:           true,
		Own:              true,
		GetExtendedAttrs: true,
		SetExtendedAttrs: true,
	}, nil
}
