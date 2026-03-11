package irodsfs

import (
	"crypto"

	"github.com/kuleuven/vfs"
)

func (fs *IRODS) Checksum(path string, algorithm crypto.Hash) ([]byte, error) {
	if algorithm != crypto.SHA256 {
		return vfs.Checksum(fs, path, algorithm)
	}

	return fs.Client.API.Checksum(fs.Context, path, false)
}
