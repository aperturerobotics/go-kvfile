package kvfile

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

// WriteValueFunc is a function that writes the value for a key to a writer.
type WriteValueFunc func(wr io.Writer, key []byte) (uint64, error)

// WriteIteratorFunc is a function that returns key/value pairs to write.
// The callback should return one key at a time.
// Return nil, nil if no keys remain.
type KeyIteratorFunc func() (key []byte, err error)

// Write writes the given key/value pairs to the store in writer.
// Serializes and writes the key/value pairs.
// Note: keys will be sorted by key.
// Note: keys must not contain duplicate keys.
// writeValue should write the given value to the writer returning the number of bytes written.
func Write(writer io.Writer, keys [][]byte, writeValue WriteValueFunc) error {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) == -1
	})

	var idx int
	return WriteIterator(writer, func() (key []byte, err error) {
		if idx >= len(keys) {
			return nil, nil
		}
		idx++
		return keys[idx-1], nil
	}, writeValue)
}

// WriteIterator writes using the given iterator callback.
// The callback should return one key at a time.
// The keys MUST be sorted or an error will be returned.
// Serializes and writes the key/value pairs.
// Note: keys must not contain duplicate keys.
func WriteIterator(writer io.Writer, keyIterator KeyIteratorFunc, writeValueFunc WriteValueFunc) error {
	// write the values and build the index
	var index []*IndexEntry
	var pos uint64
	var prevKey []byte

	for {
		nextKey, err := keyIterator()
		if err != nil {
			return err
		}
		if len(nextKey) == 0 {
			break
		}
		if len(prevKey) != 0 {
			// prevKey < nextKey is expected
			// prevKey >= nextKey is an error (not sorted or duplicate)
			cmp := bytes.Compare(prevKey, nextKey)
			if cmp == 0 {
				// skip duplicate key
				continue
			}
			if cmp > 0 {
				return errors.New("keys are not sorted")
			}
		}

		prevKey = bytes.Clone(nextKey)
		index = append(index, &IndexEntry{
			Key:    prevKey,
			Offset: pos,
		})
		nw, err := writeValueFunc(writer, prevKey)
		if err != nil {
			return err
		}
		pos += nw
	}

	// write the index entries
	indexEntryPos := make([]uint64, len(index)+1)
	var buf []byte
	for i, indexEntry := range index {
		indexEntrySize := indexEntry.SizeVT()
		if cap(buf) < indexEntrySize {
			buf = make([]byte, indexEntrySize, indexEntrySize*2)
		} else {
			buf = buf[:indexEntrySize]
		}
		_, err := indexEntry.MarshalToSizedBufferVT(buf)
		if err != nil {
			return err
		}
		// write all of buf to writer
		var nw int
		for nw < len(buf) {
			n, err := writer.Write(buf[nw:])
			if err != nil {
				return err
			}
			nw += n
		}
		// pos = the position just after the index entry
		// this is the position of the entry size varint
		pos += uint64(nw)
		indexEntryPos[i] = pos
		buf = buf[:0]
		// write the varint size of the entry
		buf = protowire.AppendVarint(buf, uint64(nw))
		nw = 0
		for nw < len(buf) {
			n, err := writer.Write(buf[nw:])
			if err != nil {
				return err
			}
			nw += n
		}
		// pos = the position just after the size varint
		pos += uint64(nw)
	}

	// write the index entry positions (fixed size uint64)
	// the last entry position is the number of entries
	indexEntryPos[len(indexEntryPos)-1] = uint64(len(index))
	for _, entryPos := range indexEntryPos {
		buf = binary.LittleEndian.AppendUint64(buf[:0], entryPos)
		nw := 0
		for nw < len(buf) {
			n, err := writer.Write(buf[nw:])
			if err != nil {
				return err
			}
			nw += n
		}
	}

	// done
	return nil
}
