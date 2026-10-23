package core

import (
	"archive/zip"
	"bytes"
	"fmt"
	"path/filepath"
)

// CheckZipPaths refuses a zip with an entry that is absolute or leaves its
// folder ("../x"). Every code-zip reader (Lambda, Azure Functions, Google
// Cloud Functions) uses it, so the same rule holds at upload and at unpack.
// Data that is not a zip is not its job: the caller reports that.
func CheckZipPaths(data []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	return CheckZipReader(zr)
}

// CheckZipReader is CheckZipPaths for a zip that is already open.
func CheckZipReader(zr *zip.Reader) error {
	for _, f := range zr.File {
		if !filepath.IsLocal(filepath.FromSlash(f.Name)) {
			return fmt.Errorf("the zip has an entry (%q) that is absolute or outside the code folder; remove it and zip again", f.Name)
		}
	}
	return nil
}
