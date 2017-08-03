package utils

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

func FilesToFileList(files []string, root string) (*os.File, error) {

	tmpfile, err := ioutil.TempFile("", "updated")

	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)

	for _, path := range files {

		abs_path := filepath.Join(root, path)

		_, ok := seen[abs_path]

		if ok {
			continue
		}

		tmpfile.Write([]byte(abs_path + "\n"))
		seen[abs_path] = true
	}

	// Remember: this is an open filehandle and you need to remember
	// to close it or hilarity will ensue...

	return tmpfile, nil
}
