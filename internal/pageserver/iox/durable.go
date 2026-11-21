package iox

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const TempSuffix = ".___temp"

type AtomicFile struct {
	f *os.File
	final string
	tmp string
	done bool
}


func Create(path string) (*AtomicFile, error) {
	tmp := path + TempSuffix

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("iox : create %s: %w", tmp, err)
	}

	return &AtomicFile{
		f: f, final: path, tmp: tmp,
	}, nil
}

func (a *AtomicFile) Write(p []byte) (int, error) {
	if a.done{
		return 0, errors.New("iox: write after commit/abort")
	}

	return a.f.Write(p)
}

func (a *AtomicFile) WriteAt(p []byte, off int64) (int, error) {
	if a.done {
		return 0, errors.New("iox: write after commit/abort")
	}

	return a.f.WriteAt(p,off)
}

func (a *AtomicFile) Commit() error {
	if a.done{
		return errors.New("iox: commit twice")
	}
	a.done = true 
	if err := a.f.Sync(); err != nil {
		a.f.Close()
		os.Remove(a.tmp)
		return fmt.Errorf("iox: fsync %s: %w", a.tmp, err)
	}

	if err := a.f.Close(); err != nil {
		os.Remove(a.tmp)
		return fmt.Errorf("iox: close %s: %w", a.tmp, err)
	}

	if err := os.Rename(a.tmp, a.final); err != nil {
		os.Remove(a.tmp )
		return fmt.Errorf("iox: rename %s: %w", a.tmp, err)
	}

	return SyncDir(filepath.Dir(a.final))
}

func (a *AtomicFile) CommitAs(path string) error  {
	if filepath.Dir(path) != filepath.Dir(a.tmp) {
		a.Abort()
		return fmt.Errorf("iox: CommitAs %s: must be in %s", path, filepath.Dir(a.tmp))
	}

	a.final = path
	return a.Commit()
}

func (a *AtomicFile) Abort() {
	if a.done {
		return
	}

	a.done = true
	a.f.Close()
	os.Remove(a.tmp)
}

func WriteFile(path string, data []byte) error {
	f, err := Create(path)
	if err != nil{
		return err
	}

	defer f.Abort()
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("iox: write %s: %w", path, err)
	}

	return f.Commit()
}

func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err!= nil {
		return fmt.Errorf("iox: open dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err !=nil {
		return fmt.Errorf("iox: fsync dir %s: %w", dir, err)
	}
	return nil
}

func CleanupTemp(dir string) (removed int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("iox: read dir %s: %w", dir, err)
	}

	for _, e := range entries {
		if e.Type().IsRegular() && strings.HasSuffix(e.Name(), TempSuffix) {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return removed, fmt.Errorf("iox: remove %s: %w", e.Name(), err)
			}

			removed++
		}
	}

	if removed > 0 {
		return removed, SyncDir(dir)
	}

	return 0, nil
}