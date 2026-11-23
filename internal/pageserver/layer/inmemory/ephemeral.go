package inmemory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

const ephemeralPrefix = "ephemeral-"

var ErrRange = errors.New("inmemory: read outside the written part of the file")

type Ephemeral struct {
	mu   sync.Mutex
	f    *os.File
	path string
	size atomic.Uint64
}

func NewEphemeral(dir string) (*Ephemeral, error) {
	f, err := os.CreateTemp(dir, ephemeralPrefix+"*")
	if err != nil {
		return nil, fmt.Errorf("inmemory: create scratch file: %w", err)
	}
	e := &Ephemeral{f: f, path: f.Name()}

	runtime.AddCleanup(e, func(f *os.File) { f.Close() }, f)
	return e, nil
}

func (e *Ephemeral) Append(b []byte) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	off := e.size.Load()
	if _, err := e.f.WriteAt(b, int64(off)); err != nil {
		return 0, fmt.Errorf("inmemory: append to %s: %w", e.path, err)
	}
	e.size.Store(off + uint64(len(b)))
	return off, nil
}

func (e *Ephemeral) ReadAt(p []byte, off uint64) error {
	if end := off + uint64(len(p)); end < off || end > e.size.Load() {
		return fmt.Errorf("%w: [%d, +%d) of %d", ErrRange, off, len(p), e.size.Load())
	}
	_, err := e.f.ReadAt(p, int64(off))
	return err
}

func (e *Ephemeral) Size() uint64 { return e.size.Load() }

func (e *Ephemeral) Remove() error {
	if err := os.Remove(e.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (e *Ephemeral) Close() error {
	err := e.f.Close()
	if rmErr := e.Remove(); err == nil {
		err = rmErr
	}
	return err
}

func RemoveLeftovers(dir string) (removed int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("inmemory: read dir %s: %w", dir, err)
	}
	for _, de := range entries {
		if de.Type().IsRegular() && strings.HasPrefix(de.Name(), ephemeralPrefix) {
			if err := os.Remove(filepath.Join(dir, de.Name())); err != nil {
				return removed, err
			}
			removed++
		}
	}
	return removed, nil
}
