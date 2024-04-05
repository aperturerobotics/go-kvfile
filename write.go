package kvfile

import (
	"bytes"
	"encoding/binary"
	"io"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

// Writer allows progressively writing values to a kvfile.
// The index will be written once the writer is closed (flushed).
// Note: keys must not contain duplicates or an error will be returned.
// Concurrency safe.
type Writer struct {
	out io.Writer
	mtx sync.Mutex
	buf []byte
	idx []*IndexEntry
	pos uint64
	fin bool
}

// NewWriter builds a new writer.
func NewWriter(out io.Writer) *Writer {
	return &Writer{out: out}
}

// WriteValue writes a key/value pair to the kvfile writer.
//
// The writer is closed if an error is returned.
func (w *Writer) WriteValue(key []byte, valueRdr io.Reader) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if w.fin {
		return errors.New("writer is already closed")
	}

	offset := w.pos
	buf := w.getBufLocked()
	nw, err := io.CopyBuffer(w.out, valueRdr, buf)
	w.pos += uint64(nw)
	if err != nil {
		if err == io.EOF {
			err = nil
		} else {
			w.fin = true
		}
	}

	w.idx = append(w.idx, &IndexEntry{
		Key:    key,
		Offset: offset,
		Size:   uint64(nw),
	})

	return err
}

// GetPos returns the current write position (written size).
func (w *Writer) GetPos() uint64 {
	return w.pos
}

// Close completes the Writer by writing the index to the file.
func (w *Writer) Close() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if w.fin {
		return errors.New("writer is already closed")
	}

	idx := w.idx
	w.fin, w.idx = true, nil
	nw, err := WriteIndex(w.out, idx, w.pos)
	w.pos += nw
	return err
}

// getBufLocked gets or allocates the scratch buffer for copies
func (w *Writer) getBufLocked() []byte {
	if len(w.buf) == 0 {
		// size from io.Copy
		w.buf = make([]byte, 32*1024)
	}
	return w.buf
}

// WriteIteratorFunc is a function that returns key/value pairs to write.
// The callback should return one key at a time in the order they should be written to the file.
// Return nil, nil or nil, io.EOF if no keys remain.
type KeyIteratorFunc func() (key []byte, err error)

// WriteValueFunc is a function that writes the value for a key to a writer.
// Return the number of bytes written and any error.
type WriteValueFunc func(wr io.Writer, key []byte) (uint64, error)

// Write writes the given key/value pairs to the store in writer.
// Serializes and writes the key/value pairs.
// Note: keys must not contain duplicates or an error will be returned.
// The values will be stored in the order of the original keys slice.
// writeValue should write the given value to the writer returning the number of bytes written.
func Write(writer io.Writer, keys [][]byte, writeValue WriteValueFunc) error {
	var idx int
	return WriteIterator(writer, func() (key []byte, err error) {
		if idx >= len(keys) {
			return nil, nil
		}
		idx++
		return keys[idx-1], nil
	}, writeValue)
}

// WriteIndex sorts and checks the index entries and writes them to a file.
//
// pos is the position the writer is located at in the file.
// returns the number of bytes written (end pos - pos).
func WriteIndex(writer io.Writer, index []*IndexEntry, pos uint64) (uint64, error) {
	startPos := pos

	// sort the index entries
	slices.SortStableFunc(index, func(a, b *IndexEntry) int {
		return bytes.Compare(a.Key, b.Key)
	})

	// write the index entries
	indexEntryPos := make([]uint64, len(index)+1)
	var buf []byte
	var prevKey []byte
	for i, indexEntry := range index {
		if i != 0 && bytes.Equal(indexEntry.Key, prevKey) {
			return pos - startPos, errors.New("duplicate key while writing")
		}
		prevKey = indexEntry.Key

		indexEntrySize := indexEntry.SizeVT()
		if cap(buf) < indexEntrySize {
			buf = make([]byte, indexEntrySize, indexEntrySize*2)
		} else {
			buf = buf[:indexEntrySize]
		}

		_, err := indexEntry.MarshalToSizedBufferVT(buf)
		if err != nil {
			return pos - startPos, err
		}

		// write all of buf to writer
		var nw int
		for nw < len(buf) {
			n, err := writer.Write(buf[nw:])
			if err != nil {
				return pos - startPos, err
			}
			nw += n
			pos += uint64(n)
		}

		// pos = the position just after the index entry
		// this is the position of the entry size varint
		indexEntryPos[i] = pos

		// write the varint size of the entry
		buf = buf[:0]
		buf = protowire.AppendVarint(buf, uint64(nw))
		nw = 0
		for nw < len(buf) {
			n, err := writer.Write(buf[nw:])
			if err != nil {
				return pos - startPos, err
			}
			nw += n
			pos += uint64(nw)
		}

		// pos = the position just after the size varint
	}

	// write the index entry positions (fixed size uint64)
	// the last entry position is the number of entries
	indexEntryPos[len(indexEntryPos)-1] = uint64(len(index))
	for _, entryPos := range indexEntryPos {
		buf = binary.LittleEndian.AppendUint64(buf[:0], entryPos)
		var nw int
		for nw < len(buf) {
			n, err := writer.Write(buf[nw:])
			if err != nil {
				return pos - startPos, err
			}
			nw += n
			pos += uint64(n)
		}
	}

	return pos - startPos, nil
}

// WriteIterator writes the key/value pairs using the given iterators.
//
// WriteValueFunc writes a value and returns number of bytes written and any error.
// WriteIteratorFunc is a function that returns key/value pairs to write.
//
// Note: keys must not contain duplicates or an error will be returned.
func WriteIterator(writer io.Writer, keyIterator KeyIteratorFunc, writeValueFunc WriteValueFunc) error {
	// write the values and build the index
	var index []*IndexEntry
	var pos uint64

	for {
		nextKey, err := keyIterator()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if len(nextKey) == 0 {
			break
		}

		offset := pos
		nw, err := writeValueFunc(writer, nextKey)
		if err != nil {
			return err
		}
		pos += nw
		index = append(index, &IndexEntry{
			Key:    nextKey,
			Offset: offset,
			Size:   nw,
		})
	}

	_, err := WriteIndex(writer, index, pos)
	if err != nil {
		return err
	}

	// done
	return nil
}
