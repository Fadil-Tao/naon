package iox_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
)

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Normal case: write, commit, file is there with the right bytes, no temp left.
func TestCommit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "layer")

	f, err := iox.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("hello "))
	f.Write([]byte("world"))

	if exists(path) {
		t.Fatal("file must not be visible before Commit")
	}
	if err := f.Commit(); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "hello world" {
		t.Fatalf("content = %q", got)
	}
	if exists(path + iox.TempSuffix) {
		t.Fatal("temp file left behind")
	}
}

// Abort: nothing appears, nothing is left behind.
func TestAbort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "layer")

	f, _ := iox.Create(path)
	f.Write([]byte("half"))
	f.Abort()

	if exists(path) || exists(path+iox.TempSuffix) {
		t.Fatal("abort must leave nothing")
	}
}

// Simulated crash: the process dies before Commit. The real file never
// appears; only a temp file remains, and CleanupTemp removes it.
func TestCrashBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "layer")

	f, _ := iox.Create(path)
	f.Write([]byte("half-written"))
	t.Cleanup(f.Abort) // "crash": no Commit, no Abort; only closes the descriptor once the test ends

	if exists(path) {
		t.Fatal("crashed file must not be visible")
	}
	n, err := iox.CleanupTemp(dir)
	if err != nil || n != 1 {
		t.Fatalf("CleanupTemp removed %d, err %v", n, err)
	}
	if exists(path + iox.TempSuffix) {
		t.Fatal("temp file not cleaned")
	}
}

// Replacing an existing file: readers see either the old or the new
// content, never a mix.
func TestWriteFileReplaces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata")

	if err := iox.WriteFile(path, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := iox.WriteFile(path, []byte("version-2")); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "version-2" {
		t.Fatalf("content = %q", got)
	}
}

// CleanupTemp leaves real files alone.
func TestCleanupKeepsRealFiles(t *testing.T) {
	dir := t.TempDir()
	iox.WriteFile(filepath.Join(dir, "keep"), []byte("x"))
	os.WriteFile(filepath.Join(dir, "junk"+iox.TempSuffix), []byte("y"), 0o644)

	n, err := iox.CleanupTemp(dir)
	if err != nil || n != 1 {
		t.Fatalf("removed %d, err %v", n, err)
	}
	if !exists(filepath.Join(dir, "keep")) {
		t.Fatal("real file was deleted")
	}
}

// Commit twice and write after commit are errors, not silent corruption.
func TestMisuse(t *testing.T) {
	f, _ := iox.Create(filepath.Join(t.TempDir(), "x"))
	if err := f.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := f.Commit(); err == nil {
		t.Fatal("second commit must fail")
	}
	if _, err := f.Write([]byte("z")); err == nil {
		t.Fatal("write after commit must fail")
	}
	f.Abort() // must be a safe no-op
}

func TestCommitAs(t *testing.T) {
	dir := t.TempDir()
	f, err := iox.Create(filepath.Join(dir, "unnamed"))
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("data"))
	final := filepath.Join(dir, "real-name")
	if err := f.CommitAs(final); err != nil {
		t.Fatal(err)
	}
	if !exists(final) || exists(filepath.Join(dir, "unnamed"+iox.TempSuffix)) || exists(filepath.Join(dir, "unnamed")) {
		t.Fatal("CommitAs should leave only the final name")
	}

	g, _ := iox.Create(filepath.Join(dir, "other"))
	if err := g.CommitAs(filepath.Join(t.TempDir(), "elsewhere")); err == nil {
		t.Fatal("CommitAs to another directory must fail")
	}
	if exists(filepath.Join(dir, "other"+iox.TempSuffix)) {
		t.Fatal("failed CommitAs must clean up the temp file")
	}
}

func TestCreateTruncatesLeftoverTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x")
	if err := os.WriteFile(path+iox.TempSuffix, bytes.Repeat([]byte("old"), 50), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := iox.WriteFile(path, []byte("new")); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "new" {
		t.Fatalf("got %q (%d bytes), want %q", got, len(got), "new")
	}
}