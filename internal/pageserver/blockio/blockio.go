// Package blockio treats a file as a row of fixed-size 8 KiB blocks.
//
//	file:  [ block 0 ][ block 1 ][ block 2 ] ...
//	bytes:  0..8191    8192..     16384..
//
// Why blocks? Disks and the OS read in fixed-size chunks, and a fixed size
// makes "read block N" a simple multiplication: offset = N * BlockSize.
// Layer files use blocks for their summary (block 0) and their index (A3).
package blockio

import (
	"errors"
	"fmt"
	"io"
)

// BlockSize is the size of one block, the same as a Postgres page.
const BlockSize = 8192

// flushThreshold is how much the Writer buffers before writing to the file.
const flushThreshold = 64 * 1024

var (
	// ErrMisaligned means a whole-block write was attempted mid-block.
	ErrMisaligned = errors.New("blockio: offset is not at a block boundary")
	// ErrBlockSize means a block was not exactly BlockSize bytes.
	ErrBlockSize = errors.New("blockio: block must be exactly BlockSize bytes")
	// ErrShortRead means the file ended before the block did.
	ErrShortRead = errors.New("blockio: block extends past end of file")
)

// ---- Writer ---------------------------------------------------------------

type Writer struct {
	w       io.WriterAt
	buf     []byte 
	flushed uint64 
	err     error
}

func NewWriter(w io.WriterAt, start uint64) *Writer {
	return &Writer{w: w, flushed: start, buf: make([]byte, 0, flushThreshold)}
}

func (w *Writer) Offset() uint64 { return w.flushed + uint64(len(w.buf)) }

func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.buf = append(w.buf, p...)
	if len(w.buf) >= flushThreshold {
		if err := w.Flush(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (w *Writer) PadToBlock() (uint32, error) {
	if rem := w.Offset() % BlockSize; rem != 0 {
		if _, err := w.Write(make([]byte, BlockSize-rem)); err != nil {
			return 0, err
		}
	}
	return w.blockNumber()
}

func (w *Writer) WriteBlock(block []byte) (uint32, error) {
	if w.err != nil {
		return 0, w.err
	}
	if len(block) != BlockSize {
		return 0, fmt.Errorf("%w: got %d", ErrBlockSize, len(block))
	}
	if w.Offset()%BlockSize != 0 {
		return 0, fmt.Errorf("%w: offset %d", ErrMisaligned, w.Offset())
	}
	blk, err := w.blockNumber()
	if err != nil {
		return 0, err
	}
	if _, err := w.Write(block); err != nil {
		return 0, err
	}
	return blk, nil
}

func (w *Writer) WriteBlockAt(blkno uint32, block []byte) error {
	if len(block) != BlockSize {
		return fmt.Errorf("%w: got %d", ErrBlockSize, len(block))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if _, err := w.w.WriteAt(block, int64(blkno)*BlockSize); err != nil {
		w.err = fmt.Errorf("blockio: write block %d: %w", blkno, err)
		return w.err
	}
	return nil
}

// Flush writes buffered bytes to the file. Call before Commit.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.w.WriteAt(w.buf, int64(w.flushed)); err != nil {
		w.err = fmt.Errorf("blockio: write at %d: %w", w.flushed, err)
		return w.err
	}
	w.flushed += uint64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}

func (w *Writer) blockNumber() (uint32, error) {
	n := w.Offset() / BlockSize
	if n > 0xFFFFFFFF {
		w.err = errors.New("blockio: file too large (block number overflow)")
		return 0, w.err
	}
	return uint32(n), nil
}

type Reader struct {
	r io.ReaderAt
}

func NewReader(r io.ReaderAt) *Reader { return &Reader{r: r} }

func (r *Reader) ReadBlock(blkno uint32, buf []byte) error {
	if len(buf) != BlockSize {
		return fmt.Errorf("%w: buffer is %d", ErrBlockSize, len(buf))
	}
	n, err := r.r.ReadAt(buf, int64(blkno)*BlockSize)
	if n == BlockSize {
		return nil // ReadAt may return io.EOF together with a full block
	}
	if err == nil || errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: block %d", ErrShortRead, blkno)
	}
	return fmt.Errorf("blockio: read block %d: %w", blkno, err)
}

func (r *Reader) Block(blkno uint32) ([]byte, error) {
	buf := make([]byte, BlockSize)
	if err := r.ReadBlock(blkno, buf); err != nil {
		return nil, err
	}
	return buf, nil
}