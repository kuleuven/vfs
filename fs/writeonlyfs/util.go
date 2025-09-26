package writeonlyfs

import (
	"errors"
	"io"

	"github.com/kuleuven/vfs"
)

var BufSize = 100

func List(fs vfs.FS, path string) ([]vfs.FileInfo, error) {
	lister, err := fs.List(path)
	if err != nil {
		return nil, err
	}

	if closer, ok := lister.(io.Closer); ok {
		defer closer.Close()
	}

	res := []vfs.FileInfo{}
	buf := make([]vfs.FileInfo, BufSize)

	var i int64

	for {
		n, err := lister.ListAt(buf, i)
		if errors.Is(err, io.EOF) {
			res = append(res, buf[:n]...)

			return res, nil
		}

		if err != nil {
			return nil, err
		}

		i += int64(BufSize)

		res = append(res, buf[:n]...)
	}
}
