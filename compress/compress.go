package kvfile_compress

import (
	"io"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	kvfile "github.com/aperturerobotics/go-kvfile"
	"github.com/klauspost/compress/zstd"
)

// WriteCompress writes the given key/value pairs to the store in writer.
// Uses seekable zstd compression.
//
// Serializes and writes the key/value pairs.
// Note: keys will be sorted by key.
// Note: keys must not contain duplicate keys.
// writeValue should write the given value to the writer returning the number of bytes written.
func WriteCompress(writer io.Writer, keys [][]byte, writeValue func(wr io.Writer, key []byte) (uint64, error)) error {
	zenc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return err
	}
	defer zenc.Close()

	w, err := seekable.NewWriter(writer, zenc)
	if err != nil {
		return err
	}

	if err := kvfile.Write(w, keys, writeValue); err != nil {
		return err
	}

	// Close and flush seek table.
	return w.Close()
}

// ReadSeekerAt is the interface BuildCompressReader accepts.
type ReadSeekerAt interface {
	io.ReadSeeker
	io.ReaderAt
}

// BuildCompressReader reads key/value pairs from the compressed reader.
// Uses seekable zstd compression.
func BuildCompressReader(rd ReadSeekerAt) (*kvfile.Reader, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	r, err := seekable.NewReader(rd, dec)
	if err != nil {
		dec.Close()
		return nil, err
	}
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		dec.Close()
		_ = r.Close()
		return nil, err
	}
	kvReader, err := kvfile.BuildReader(r, uint64(size))
	if err != nil {
		dec.Close()
		_ = r.Close()
		return nil, err
	}
	return kvReader, nil
}
