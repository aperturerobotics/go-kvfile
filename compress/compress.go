package kvfile_compress

import (
	"io"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	kvfile "github.com/aperturerobotics/go-kvfile"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

// UseCompressedWriter builds a compressed writer and closes it after the
// callback returns.
func UseCompressedWriter(writer io.Writer, cb func(writer io.Writer) error) error {
	zenc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return err
	}
	defer zenc.Close()

	w, err := seekable.NewWriter(writer, zenc)
	if err != nil {
		return err
	}

	if err = cb(w); err != nil {
		_ = w.Close()
		return err
	}

	return w.Close()
}

// WriteCompress writes the given key/value pairs to the store in writer.
// Uses seekable zstd compression.
//
// Serializes and writes the key/value pairs.
// Note: keys will be sorted by key.
// Note: keys must not contain duplicate keys.
// writeValue should write the given value to the writer returning the number of bytes written.
func WriteCompress(writer io.Writer, keys [][]byte, writeValue func(wr io.Writer, key []byte) (uint64, error)) error {
	return UseCompressedWriter(writer, func(w io.Writer) error {
		return kvfile.Write(w, keys, writeValue)
	})
}

// ReadSeekerAt is the interface BuildCompressReader accepts.
type ReadSeekerAt interface {
	io.ReadSeeker
	io.ReaderAt
}

// BuildCompressReader reads key/value pairs from the compressed reader.
// Uses seekable zstd compression.
// Returns a function to call to release the zstd reader.
func BuildCompressReader(rd ReadSeekerAt) (*kvfile.Reader, func(), error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, nil, err
	}
	r, err := seekable.NewReader(rd, dec)
	if err != nil {
		dec.Close()
		return nil, nil, err
	}
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		dec.Close()
		_ = r.Close()
		return nil, nil, err
	}
	// Check non-negative before conversion
	if size < 0 {
		dec.Close()
		_ = r.Close()
		return nil, nil, errors.Errorf("seek returned negative size: %d", size)
	}
	kvReader, err := kvfile.BuildReader(r, uint64(size))
	if err != nil {
		dec.Close()
		_ = r.Close()
		return nil, nil, err
	}
	return kvReader, func() {
		dec.Close()
		_ = r.Close()
	}, nil
}
