package terraform

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Load reads path: a folder of .tf files, or a plan JSON file (terraform show -json).
// varFiles are extra .tfvars files, relative to the folder or absolute.
func Load(path string, varFiles []string) (*Config, error) {
	files, keys, plan, err := ReadFiles(path, varFiles)
	if err != nil {
		return nil, err
	}
	if plan {
		for _, b := range files {
			return ReadPlan(b)
		}
	}
	return Parse(files, keys)
}

// ReadFiles is the reader Load uses, for callers that send the files somewhere else (pogi tf diagram).
// It returns the files by slash path (a plan is one file under its own name), the keys of the var
// files in order, and whether path is a plan. It applies the same limits as Load.
func ReadFiles(path string, varFiles []string) (files map[string][]byte, keys []string, plan bool, err error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, false, fmt.Errorf("%s was not found", path) // not "stat /x: no such file or directory"
	}
	if err != nil {
		return nil, nil, false, err
	}
	if !info.IsDir() {
		if info.Size() > MaxBytes {
			return nil, nil, false, fmt.Errorf("%s is too big: %d MB (the limit is %d MB)", path, info.Size()>>20, MaxBytes>>20)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, false, err
		}
		if !IsPlan(data) {
			return nil, nil, false, fmt.Errorf("%s is not a Terraform plan: pass a folder of .tf files, or a plan JSON (%s)", path, planHint)
		}
		return map[string][]byte{filepath.Base(path): data}, nil, true, nil
	}
	files, err = readFolder(path)
	if err != nil {
		return nil, nil, false, err
	}
	for _, v := range varFiles {
		key := filepath.ToSlash(filepath.Clean(v))
		full := v
		if !filepath.IsAbs(v) {
			full = filepath.Join(path, v)
		}
		if _, ok := files[key]; !ok {
			fi, err := os.Stat(full)
			if err != nil || fi.IsDir() {
				return nil, nil, false, fmt.Errorf("var file %q was not found", v)
			}
			if fi.Size() > MaxBytes {
				return nil, nil, false, fmt.Errorf("var file %q is too big", v)
			}
			b, err := os.ReadFile(full)
			if err != nil {
				return nil, nil, false, fmt.Errorf("var file %q: %w", v, err)
			}
			files[key] = b
		}
		keys = append(keys, key)
	}
	return files, keys, false, nil
}

// readFolder reads the .tf, .tf.json, .tfvars and .tfvars.json files under root, and
// .terraform/modules (modules.json and the downloaded modules). It skips other hidden folders,
// node_modules and symlinks, and stops at the file and size limits.
func readFolder(root string) (map[string][]byte, error) {
	files := map[string][]byte{}
	total := int64(0)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		if d.IsDir() {
			switch {
			case rel == ".":
			case rel == ".terraform" || rel == ".terraform/modules" || strings.HasPrefix(rel, ".terraform/modules/"):
			case strings.HasPrefix(d.Name(), ".") || d.Name() == "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		want := strings.HasSuffix(name, ".tf") || strings.HasSuffix(name, ".tf.json") ||
			strings.HasSuffix(name, ".tfvars") || strings.HasSuffix(name, ".tfvars.json") ||
			rel == ".terraform/modules/modules.json"
		if !want || (strings.HasPrefix(rel, ".terraform/") && !strings.HasPrefix(rel, ".terraform/modules/")) {
			return nil
		}
		if len(files) >= MaxFiles {
			return fmt.Errorf("too many files in %s: more than %d", root, MaxFiles)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if total += info.Size(); total > MaxBytes {
			return fmt.Errorf("the files in %s are too big: more than %d MB", root, MaxBytes>>20)
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, MaxBytes+1))
		if err != nil {
			return err
		}
		files[rel] = b
		return nil
	})
	return files, err
}
