package kvfile

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

// maxIndexEntrySize is the maximum index entry size in bytes
// to avoid overflow attacks
// this is also an upper bound on key length
var maxIndexEntrySize = 2048

// maxValueSize is the maximum value size we will read
// currently set to 100Mb
var maxValueSize uint32 = 1e8

// Reader is a key/value file reader.
type Reader struct {
	// rd is the reader
	rd io.ReaderAt
	// indexEntryCount is the number of entries in the index entries list.
	// if 0, the file is empty
	indexEntryCount uint64
	// indexEntryIndexesPos is the position in the file of the index entries indexes list.
	indexEntryIndexesPos uint64
	// indexEntryListPos is the position in the file of the first index entry.
	indexEntryListPos uint64
}

// BuildReader constructs a new Reader, reading the number of index entries.
func BuildReader(rd io.ReaderAt, fileSize uint64) (*Reader, error) {
	if fileSize == 0 {
		return &Reader{rd: rd, indexEntryCount: 0}, nil
	}

	// read the number of index entries
	indexEntryCountPos := int64(fileSize) - 8
	buf := make([]byte, 8)
	_, err := rd.ReadAt(buf, indexEntryCountPos)
	if err != nil {
		return nil, err
	}
	indexEntryCount := binary.LittleEndian.Uint64(buf)
	indexEntryIndexesPos := indexEntryCountPos - int64(indexEntryCount*8)
	if indexEntryIndexesPos < 0 {
		return nil, errors.Errorf("invalid count of index entries for file size: %v", indexEntryCount)
	}
	// read the first index entry pos
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	_, err = rd.ReadAt(buf, indexEntryIndexesPos)
	if err != nil {
		return nil, err
	}
	firstIndexEntryLenPos := binary.LittleEndian.Uint64(buf)
	// read the size of the first index entry
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	_, err = rd.ReadAt(buf, int64(firstIndexEntryLenPos))
	if err != nil {
		return nil, err
	}
	indexEntrySize, indexEntrySizeLen := protowire.ConsumeVarint(buf)
	if indexEntrySizeLen < 0 {
		return nil, errors.Errorf("invalid index entry size varint at %v", firstIndexEntryLenPos)
	}
	if indexEntrySize > uint64(maxIndexEntrySize) {
		return nil, errors.Errorf("invalid index entry size at %v: %v > %v", firstIndexEntryLenPos, indexEntrySize, maxIndexEntrySize)
	}
	// determine the position of the first IndexEntry entry
	indexEntryListPos := int64(firstIndexEntryLenPos) - int64(indexEntrySize)
	if indexEntryListPos < 0 {
		return nil, errors.Errorf("invalid index entry size at %v: %v > %v", firstIndexEntryLenPos, indexEntrySize, firstIndexEntryLenPos)
	}
	return &Reader{
		rd:                   rd,
		indexEntryCount:      indexEntryCount,
		indexEntryIndexesPos: uint64(indexEntryIndexesPos),
		indexEntryListPos:    uint64(indexEntryListPos),
	}, nil
}

// ReadIndexEntry reads the index entry at the given index.
func (r *Reader) ReadIndexEntry(indexEntryIdx uint64) (*IndexEntry, error) {
	if indexEntryIdx > r.indexEntryCount {
		return nil, errors.Errorf("out-of-bounds read of index entry: %v > %v", indexEntryIdx, r.indexEntryCount)
	}
	// determine the position of the entry in the positions list
	indexEntryLocPos := r.indexEntryIndexesPos + (8 * indexEntryIdx)
	// read the entry position
	buf := make([]byte, 8, 10)
	_, err := r.rd.ReadAt(buf, int64(indexEntryLocPos))
	if err != nil {
		return nil, err
	}
	// determine the position of the index entry size varint
	indexEntrySizePos := binary.LittleEndian.Uint64(buf)
	// read the index entry size varint
	buf = buf[:10]
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	_, err = r.rd.ReadAt(buf, int64(indexEntrySizePos))
	if err != nil {
		return nil, err
	}
	indexEntrySize, indexEntrySizeLen := protowire.ConsumeVarint(buf)
	if indexEntrySizeLen < 0 {
		return nil, errors.Errorf("invalid index entry size varint at %v", indexEntrySizePos)
	}
	if indexEntrySize > uint64(maxIndexEntrySize) {
		return nil, errors.Errorf("invalid index entry size at %v: %v > %v", indexEntrySizePos, indexEntrySize, maxIndexEntrySize)
	}
	buf = make([]byte, indexEntrySize)
	indexEntryPos := int64(indexEntrySizePos) - int64(indexEntrySize)
	_, err = r.rd.ReadAt(buf, indexEntryPos)
	if err != nil {
		return nil, err
	}
	indexEntry := &IndexEntry{}
	if err := indexEntry.UnmarshalVT(buf); err != nil {
		return nil, errors.Errorf("invalid index entry at %v: %v", indexEntryPos, err.Error())
	}
	if off := indexEntry.GetOffset(); off > uint64(indexEntryPos) {
		return nil, errors.Errorf("invalid index entry at %v: offset %v is greater than index entry pos", indexEntryPos, off)
	}
	return indexEntry, nil
}

// SearchIndexEntry looks up an index entry for the given key.
// Returns nil, -1, nil, if not found.
func (r *Reader) SearchIndexEntry(key []byte) (*IndexEntry, int, error) {
	var matchedEntry *IndexEntry
	var matchedIdx int
	var entry *IndexEntry
	var err error
	searchIdx := sort.Search(int(r.indexEntryCount), func(idx int) bool {
		if err != nil || matchedEntry != nil {
			return false
		}
		entry, err = r.ReadIndexEntry(uint64(idx))
		if err != nil {
			return false
		}
		cmp := bytes.Compare(entry.GetKey(), key)
		if cmp == 0 {
			matchedEntry = entry
			matchedIdx = idx
		}
		return cmp >= 0
	})
	if err != nil {
		return nil, -1, err
	}
	if matchedEntry != nil {
		return matchedEntry, matchedIdx, nil
	}
	if searchIdx >= int(r.indexEntryCount) || searchIdx < 0 {
		return nil, -1, nil
	}
	entry, err = r.ReadIndexEntry(uint64(searchIdx))
	if err != nil {
		return nil, searchIdx, err
	}
	if !bytes.Equal(entry.GetKey(), key) {
		return nil, -1, nil
	}
	return entry, searchIdx, nil
}

// Exists checks if the given key exists in the store.
func (r *Reader) Exists(key []byte) (bool, error) {
	_, idx, err := r.SearchIndexEntry(key)
	return idx >= 0, err
}

// GetValuePosition determines the position and length of the value for the key.
//
// Returns -1, 1, nil, -1, nil if not found.
func (r *Reader) GetValuePosition(key []byte) (idx, length int64, indexEntry *IndexEntry, indexEntryIdx int, err error) {
	indexEntry, indexEntryIdx, err = r.SearchIndexEntry(key)
	if indexEntryIdx < 0 {
		return -1, -1, nil, -1, err
	}

	// determine the end of the data
	var valueEnd int64
	if indexEntryIdx+1 >= int(r.indexEntryCount) {
		// last value: ends just before the first index entry
		valueEnd = int64(r.indexEntryListPos)
	} else {
		// get the offset of the value after this one
		nextIndexEntry, err := r.ReadIndexEntry(uint64(indexEntryIdx) + 1)
		if err != nil {
			return -1, -1, indexEntry, indexEntryIdx, err
		}
		valueEnd = int64(nextIndexEntry.GetOffset())
		if valueEnd > int64(r.indexEntryListPos) {
			return -1, -1, indexEntry, indexEntryIdx, errors.Errorf("invalid offset of index entry: %v > list pos %v", valueEnd, r.indexEntryListPos)
		}
	}

	valueOffset := int64(indexEntry.GetOffset())
	if valueOffset > valueEnd {
		return -1, -1, indexEntry, indexEntryIdx, errors.Errorf("invalid offset of index entry: %v > value end %v", valueOffset, valueEnd)
	}

	valueLen := valueEnd - valueOffset
	if valueLen > int64(maxValueSize) {
		return -1, -1, indexEntry, indexEntryIdx, errors.Errorf("value size %v > max size %v", valueLen, maxValueSize)
	}

	return valueOffset, valueLen, indexEntry, indexEntryIdx, nil
}

// Get looks up the value for the given key.
// Returns nil, false, nil if not found
func (r *Reader) Get(key []byte) ([]byte, bool, error) {
	valueIdx, valueLen, _, _, err := r.GetValuePosition(key)
	if err != nil || valueLen < 0 || valueIdx < 0 {
		return nil, false, err
	}
	readBuf := make([]byte, valueLen)
	_, err = r.rd.ReadAt(readBuf, valueIdx)
	if err != nil {
		return nil, true, err
	}
	return readBuf, true, nil
}

// ReadTo reads the value for the given key to the writer.
//
// Returns number of bytes read, found, and any error.
// Returns 0, false, nil if not found.
func (r *Reader) ReadTo(key []byte, to io.Writer) (int, bool, error) {
	valueIdx, valueLen, _, _, err := r.GetValuePosition(key)
	if err != nil || valueLen < 0 || valueIdx < 0 {
		return 0, false, err
	}
	readBufSize := 2048
	if vl := int(valueLen); vl < readBufSize {
		readBufSize = vl
	}
	readBuf := make([]byte, readBufSize)
	pos := valueIdx
	var nr int64
	for nr < valueLen {
		remaining := valueLen - nr
		if len(readBuf) > int(remaining) {
			readBuf = readBuf[:remaining]
		}
		nread, err := r.rd.ReadAt(readBuf, pos)
		if err != nil {
			return 0, true, err
		}
		var nw int
		for nw < nread && nw < len(readBuf) {
			njw, err := to.Write(readBuf[nw:])
			if err != nil {
				return 0, true, err
			}
			nw += njw
		}
		pos += int64(nread)
		nr += int64(nw)
	}
	return int(nr), true, nil
}

// KeyValue is a key-value pair.
type KeyValue struct {
	// Key is the key to store.
	Key []byte
	// GetValue returns the value to store.
	GetValue func(key []byte) ([]byte, error)
}

// Write writes the given key/value pairs to the store in writer.
// Serializes and writes the key/value pairs.
// Note: keys will be sorted by key.
// Note: keys must not contain duplicate keys.
// writeValue should write the given value to the writer returning the number of bytes written.
func Write(writer io.Writer, keys [][]byte, writeValue func(wr io.Writer, key []byte) (uint64, error)) error {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) == -1
	})

	// write the values and build the index
	index := make([]*IndexEntry, 0, len(keys))
	var pos uint64
	var prevKey []byte
	for i, key := range keys {
		if i != 0 && bytes.Equal(prevKey, key) {
			// skip duplicate key
			continue
		}
		index = append(index, &IndexEntry{
			Key:    key,
			Offset: pos,
		})
		nw, err := writeValue(writer, key)
		if err != nil {
			return err
		}
		pos += nw
		prevKey = key
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
