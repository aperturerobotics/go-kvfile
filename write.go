package kvfile

import (
	"bytes"
	"encoding/binary"
	"io"
	"slices"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

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

		index = append(index, &IndexEntry{
			Key:    nextKey,
			Offset: pos,
		})
		nw, err := writeValueFunc(writer, nextKey)
		if err != nil {
			return err
		}
		pos += nw
	}

	// sort the index entries
	slices.SortStableFunc(index, func(a, b *IndexEntry) int {
		return bytes.Compare(a.Key, b.Key)
	})

	// check for duplicates (not allowed)
	var prevKey []byte
	for i, ent := range index {
		if i != 0 && bytes.Equal(ent.Key, prevKey) {
			return errors.New("duplicate key while writing")
		}
		prevKey = ent.Key
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
