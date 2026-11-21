// Package blobio stores variable-length values ("blobs") one after another in
// a file, and finds them again by their byte offset.
package blobio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/Fadil-Tao/naon/internal/pageserver/blockio"
)

const (
	// MaxBlobLen is the largest blob: 28 bits of length (256 MiB - 1).
	MaxBlobLen = 0x0FFF_FFFF

	compressionMask      = 0xF0 // top 4 bits of a long header's first byte
	compressionNone      = 0x80 // "long header, not compressed"
	compressionZstd      = 0x90 // reserved for later (Neon uses it for images)
	shortHeaderMaxLength = 0x7F
)

var (
	// ErrTooLarge means a blob is longer than MaxBlobLen.
	ErrTooLarge = errors.New("blobio: blob too large")
	// ErrCorrupt means a header or length does not make sense.
	ErrCorrupt = errors.New("blobio: corrupt blob")
	// ErrUnsupportedCompression means the header asks for compression we
	// don't implement yet.
	ErrUnsupportedCompression = errors.New("blobio: unsupported compression")
)

// appendHeader adds the length header for a blob of length n.
func appendHeader(dst []byte, n int) []byte {
	if n <= shortHeaderMaxLength {
		return append(dst, byte(n))
	}
	return binary.BigEndian.AppendUint32(dst, uint32(n)|uint32(compressionNone)<<24)
}

// HeaderLen is the size of the header for a blob of length n (1 or 4).
func HeaderLen(n int) int {
	if n <= shortHeaderMaxLength {
		return 1
	}
	return 4
}

// ---- Writer ---------------------------------------------------------------

// Writer appends blobs through a blockio.Writer.
type Writer struct {
	w   *blockio.Writer
	hdr []byte // reused header buffer
}

// NewWriter appends blobs to w, starting at w's current offset.
func NewWriter(w *blockio.Writer) *Writer { return &Writer{w: w, hdr: make([]byte, 0, 4)} }

// WriteBlob appends one blob and returns its offset (its address).
func (bw *Writer) WriteBlob(data []byte) (uint64, error) {
	if len(data) > MaxBlobLen {
		return 0, fmt.Errorf("%w: %d bytes", ErrTooLarge, len(data))
	}
	off := bw.w.Offset()
	bw.hdr = appendHeader(bw.hdr[:0], len(data))
	if _, err := bw.w.Write(bw.hdr); err != nil {
		return 0, err
	}
	if _, err := bw.w.Write(data); err != nil {
		return 0, err
	}
	return off, nil
}

// ---- Reader ---------------------------------------------------------------

type Reader struct {
	r    io.ReaderAt
	size uint64 // file size; used to reject corrupt lengths before allocating
}

func NewReader(r io.ReaderAt, size int64) *Reader {
	return &Reader{r: r, size: uint64(size)}
}

func (br *Reader) ReadBlob(off uint64) ([]byte, error) {
	return br.ReadBlobInto(nil, off)
}

func (br *Reader) ReadBlobInto(dst []byte, off uint64) ([]byte, error) {
	length, hdrLen, err := br.readHeader(off)
	if err != nil {
		return dst, err
	}
	start := off + uint64(hdrLen)
	if start+uint64(length) > br.size {
		return dst, fmt.Errorf("%w: blob at %d (len %d) runs past end of file (%d)", ErrCorrupt, off, length, br.size)
	}
	n := len(dst)
	dst = append(dst, make([]byte, length)...)
	if err := br.readFull(dst[n:], start); err != nil {
		return dst[:n], err
	}
	return dst, nil
}

func (br *Reader) readHeader(off uint64) (length uint32, hdrLen int, err error) {
	if off >= br.size {
		return 0, 0, fmt.Errorf("%w: offset %d is past end of file (%d)", ErrCorrupt, off, br.size)
	}
	var hdr [4]byte
	if err := br.readFull(hdr[:1], off); err != nil {
		return 0, 0, err
	}
	if hdr[0] < 0x80 { // short header: the byte is the length
		return uint32(hdr[0]), 1, nil
	}
	switch hdr[0] & compressionMask {
	case compressionNone:
	case compressionZstd:
		return 0, 0, fmt.Errorf("%w: zstd at offset %d", ErrUnsupportedCompression, off)
	default:
		return 0, 0, fmt.Errorf("%w: unknown header byte %#x at offset %d", ErrCorrupt, hdr[0], off)
	}
	if err := br.readFull(hdr[1:], off+1); err != nil {
		return 0, 0, err
	}
	length = binary.BigEndian.Uint32(hdr[:]) &^ (uint32(compressionMask) << 24)
	return length, 4, nil
}

// readFull fills buf from offset off, or fails.
func (br *Reader) readFull(buf []byte, off uint64) error {
	n, err := br.r.ReadAt(buf, int64(off))
	if n == len(buf) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: short read at offset %d", ErrCorrupt, off)
	}
	return fmt.Errorf("blobio: read at %d: %w", off, err)
}