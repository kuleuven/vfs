package sftpfs

import (
	"os"
	"strings"
	"syscall"

	"github.com/kuleuven/vfs"
	"github.com/pkg/sftp"
)

func NormalizeError(err error) error {
	if err == nil {
		return nil
	}

	if sftpErr, ok := err.(*sftp.StatusError); ok {
		return sftpError(sftpErr)
	}

	return err
}

func sftpError(err *sftp.StatusError) error {
	switch err.Code {
	case 8: // SSH_FX_OP_UNSUPPORTED
		return vfs.ErrNotSupported
	case 24: // sshFxFileIsADirectory
		return syscall.EISDIR
	case 19: // sshFxNotADirectory
		return syscall.ENOTDIR
	case 9: // sshFxInvalidHandle
		return vfs.ErrInvalidHandle
	case 20: // sshFxInvalidFilename
		return os.ErrInvalid
	case 11: // sshFxFileAlreadyExists
		return os.ErrExist
	default:
		if strings.Contains(err.Error(), "permission denied") {
			return os.ErrPermission
		}

		if strings.Contains(err.Error(), "not a directory") {
			return syscall.ENOTDIR
		}

		if strings.Contains(err.Error(), "no such file") {
			return os.ErrNotExist
		}

		if strings.Contains(err.Error(), "invalid handle") {
			return vfs.ErrInvalidHandle
		}

		if strings.Contains(err.Error(), "operation unsupported") {
			return vfs.ErrNotSupported
		}

		return err
	}
}
