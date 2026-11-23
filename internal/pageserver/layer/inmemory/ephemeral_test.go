package inmemory

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestEphemeralAppendRead(t *testing.T) {
	e, err := NewEphemeral(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	var offs []uint64
	chunks := [][]byte{[]byte("hello"), {}, bytes.Repeat([]byte{7}, 10_000), []byte("x")}
	want := uint64(0)
	for _, c := range chunks {
		off, err := e.Append(c)
		if err != nil {
			t.Fatal(err)
		}
		if off != want {
			t.Fatalf("offset = %d, want %d", off, want)
		}
		offs = append(offs, off)
		want += uint64(len(c))
	}
	if e.Size() != want {
		t.Fatalf("size = %d, want %d", e.Size(), want)
	}
	for i, c := range chunks {
		got := make([]byte, len(c))
		if err := e.ReadAt(got, offs[i]); err != nil || !bytes.Equal(got, c) {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}
}

func TestEphemeralReadPastEnd(t *testing.T) {
	e, _ := NewEphemeral(t.TempDir())
	defer e.Close()
	e.Append([]byte("abc"))
	for _, c := range []struct{ off, n uint64 }{{0, 4}, {3, 1}, {4, 0}, {1 << 63, 1}, {^uint64(0), 2}} {
		if err := e.ReadAt(make([]byte, c.n), c.off); !errors.Is(err, ErrRange) {
			t.Fatalf("ReadAt(%d, +%d): err = %v, want ErrRange", c.off, c.n, err)
		}
	}

	if err := e.ReadAt(make([]byte, 3), 0); err != nil {
		t.Fatal(err)
	}
	if err := e.ReadAt(nil, 3); err != nil {
		t.Fatal(err)
	}
}

func TestEphemeralConcurrent(t *testing.T) {
	e, _ := NewEphemeral(t.TempDir())
	defer e.Close()

	const n = 2000
	offs := make([]uint64, n)
	var published sync.Mutex
	count := 0

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				published.Lock()
				c := count
				var off uint64
				if c > 0 {
					off = offs[c-1]
				}
				published.Unlock()
				if c == 0 {
					continue
				}
				got := make([]byte, 8)
				if err := e.ReadAt(got, off); err != nil {
					t.Errorf("read: %v", err)
					return
				}
				if want := bytes.Repeat([]byte{byte(c - 1)}, 8); !bytes.Equal(got, want) {
					t.Errorf("entry %d = %v", c-1, got)
					return
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		off, err := e.Append(bytes.Repeat([]byte{byte(i)}, 8))
		if err != nil {
			t.Fatal(err)
		}
		published.Lock()
		offs[i] = off
		count = i + 1
		published.Unlock()
	}
	close(stop)
	wg.Wait()
}

func TestEphemeralRemoveKeepsReadsWorking(t *testing.T) {
	dir := t.TempDir()
	e, _ := NewEphemeral(dir)
	e.Append([]byte("data"))
	if es, _ := os.ReadDir(dir); len(es) != 1 {
		t.Fatalf("want 1 file, got %d", len(es))
	}
	if err := e.Remove(); err != nil {
		t.Fatal(err)
	}
	if err := e.Remove(); err != nil {
		t.Fatal(err)
	}
	if es, _ := os.ReadDir(dir); len(es) != 0 {
		t.Fatalf("file still in the directory")
	}
	got := make([]byte, 4)
	if err := e.ReadAt(got, 0); err != nil || string(got) != "data" {
		t.Fatalf("read after Remove: %q, %v", got, err)
	}
	e.Close()
}

func TestEphemeralClose(t *testing.T) {
	dir := t.TempDir()
	e, _ := NewEphemeral(dir)
	e.Append([]byte("data"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if es, _ := os.ReadDir(dir); len(es) != 0 {
		t.Fatal("Close left the file")
	}
	if err := e.ReadAt(make([]byte, 4), 0); err == nil {
		t.Fatal("read after Close succeeded")
	}
}

func TestRemoveLeftovers(t *testing.T) {
	dir := t.TempDir()
	e1, _ := NewEphemeral(dir)
	e2, _ := NewEphemeral(dir)
	defer e1.Close()
	defer e2.Close()
	keep := filepath.Join(dir, "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000000000001-0000000000000002")
	if err := os.WriteFile(keep, []byte("layer"), 0o644); err != nil {
		t.Fatal(err)
	}
	n, err := RemoveLeftovers(dir)
	if err != nil || n != 2 {
		t.Fatalf("removed %d, %v; want 2", n, err)
	}
	if es, _ := os.ReadDir(dir); len(es) != 1 || es[0].Name() != filepath.Base(keep) {
		t.Fatalf("directory after cleanup: %v", es)
	}
}
