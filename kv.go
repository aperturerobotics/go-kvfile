package kvfile

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/fs"
	"math"

	protobuf_go_lite "github.com/aperturerobotics/protobuf-go-lite"
	"github.com/pkg/errors"
)

// maxIndexEntrySize is the maximum index entry size in bytes
// to avoid overflow attacks
// this is also an upper bound on key length
var maxIndexEntrySize = 2048

// maxValueSize is the maximum value size we will read
// currently set to 1GB
var maxValueSize uint32 = 1e9

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
	if indexEntryCount > math.MaxUint64/8 || indexEntryCount*8 > uint64(indexEntryCountPos) {
		return nil, errors.Errorf("index entry count too large: %v", indexEntryCount)
	}
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
	indexEntrySize, indexEntrySizeLen := protobuf_go_lite.ConsumeVarint(buf)
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

// ReaderAtSeeker is a ReaderAt and a ReadSeeker.
type ReaderAtSeeker interface {
	io.ReaderAt
	io.ReadSeeker
}

// BuildReaderWithSeeker constructs a new Reader with a io.ReaderSeeker reading the file size.
func BuildReaderWithSeeker(rd ReaderAtSeeker) (*Reader, error) {
	size, err := rd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if size != 0 {
		_, err = rd.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
	}
	return BuildReader(rd, uint64(size))
}

// FileReaderAt is a fs.File that implements ReaderAt.
type FileReaderAt interface {
	fs.File
	io.ReaderAt
}

// BuildReaderWithFile constructs a new Reader with an fs.File.
func BuildReaderWithFile(f FileReaderAt) (*Reader, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	return BuildReader(f, uint64(size))
}

// ReadIndexEntry reads the index entry at the given index.
func (r *Reader) ReadIndexEntry(indexEntryIdx uint64) (*IndexEntry, error) {
	if indexEntryIdx >= r.indexEntryCount {
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
	indexEntrySize, indexEntrySizeLen := protobuf_go_lite.ConsumeVarint(buf)
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

// SearchIndexEntryWithKey looks up an index entry for the given key.
//
// If not found, returns nil, idx, err and idx is the index where the searched
// element would appear if inserted into the list.
func (r *Reader) SearchIndexEntryWithKey(key []byte) (*IndexEntry, int, error) {
	var entry *IndexEntry
	var err error

	// binary search from sort.Search
	i, j := 0, int(r.indexEntryCount)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h

		entry, err = r.ReadIndexEntry(uint64(h))
		if err != nil {
			return nil, h, err
		}

		cmp := bytes.Compare(entry.GetKey(), key)
		if cmp == 0 {
			return entry, h, nil
		}

		if cmp < 0 {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}

	return nil, i, nil
}

// SearchIndexEntryWithPrefix returns the entry of the first key with the prefix.
//
// If last is true returns the last element that matches the prefix.
//
// If the key or prefix is not found, returns nil, idx, err, where idx is the
// element where an element with the given prefix would appear if inserted.
func (r *Reader) SearchIndexEntryWithPrefix(prefix []byte, last bool) (*IndexEntry, int, error) {
	// if len(prefix) is empty return the first or last element of the whole set.
	if len(prefix) == 0 {
		idx := 0
		if last {
			idx = int(r.indexEntryCount) - 1
		}
		if idx >= 0 && idx < int(r.indexEntryCount) {
			ent, err := r.ReadIndexEntry(uint64(idx))
			return ent, idx, err
		}
		return nil, idx, nil
	}

	i, j := 0, int(r.indexEntryCount)-1
	var matchedEntry *IndexEntry
	var matchedIdx int
	for i <= j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h

		entry, err := r.ReadIndexEntry(uint64(h))
		if err != nil {
			return nil, h, err
		}

		key := entry.GetKey()
		if bytes.HasPrefix(key, prefix) {
			matchedEntry, matchedIdx = entry, h
			if last {
				i = h + 1
			} else {
				j = h - 1
			}
		} else {
			cmp := bytes.Compare(key, prefix)
			if cmp < 0 {
				i = h + 1
			} else {
				j = h - 1
			}
		}
	}
	if matchedEntry != nil {
		return matchedEntry, matchedIdx, nil
	}
	return nil, i, nil
}

// Size returns the number of key/value pairs in the store.
func (r *Reader) Size() uint64 {
	return r.indexEntryCount
}

// Exists checks if the given key exists in the store.
func (r *Reader) Exists(key []byte) (bool, error) {
	elem, _, err := r.SearchIndexEntryWithKey(key)
	return elem != nil, err
}

// GetValuePositionWithEntry determines the position and length of the value with an entry.
//
// Returns -1, 1, nil, -1, nil if not found.
func (r *Reader) GetValuePositionWithEntry(indexEntry *IndexEntry, indexEntryIdx int) (idx, length int64, err error) {
	valueOffset := int64(indexEntry.GetOffset())
	valueSize := int64(indexEntry.GetSize())
	if valueSize > int64(maxValueSize) {
		return -1, -1, errors.Errorf("value size %v > max size %v", valueSize, maxValueSize)
	}
	valueEnd := valueOffset + valueSize
	if valueEnd < valueSize || valueEnd >= int64(r.indexEntryIndexesPos) {
		return -1, -1, errors.Errorf("value size %v out of bounds", valueSize)
	}
	return valueOffset, valueSize, nil
}

// GetValuePosition determines the position and length of the value for the key.
//
// Returns -1, 1, nil, -1, nil if not found.
func (r *Reader) GetValuePosition(key []byte) (idx, length int64, indexEntry *IndexEntry, indexEntryIdx int, err error) {
	indexEntry, indexEntryIdx, err = r.SearchIndexEntryWithKey(key)
	if indexEntry == nil {
		return -1, -1, nil, -1, err
	}
	idx, length, err = r.GetValuePositionWithEntry(indexEntry, indexEntryIdx)
	return idx, length, indexEntry, indexEntryIdx, err
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

// GetWithEntry returns the value for the given index entry.
func (r *Reader) GetWithEntry(indexEntry *IndexEntry, indexEntryIdx int) ([]byte, error) {
	valueIdx, valueLen, err := r.GetValuePositionWithEntry(indexEntry, indexEntryIdx)
	if err == nil && (valueLen < 0 || valueIdx < 0) {
		err = errors.New("entry value not found")
	}
	if err != nil {
		return nil, err
	}
	readBuf := make([]byte, valueLen)
	_, err = r.rd.ReadAt(readBuf, valueIdx)
	if err != nil {
		return nil, err
	}
	return readBuf, nil
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
		remaining := int(valueLen - nr)
		if len(readBuf) > remaining {
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

// ScanPrefixEntries iterates over entries with the given key prefix.
func (r *Reader) ScanPrefixEntries(prefix []byte, cb func(indexEntry *IndexEntry, indexEntryIdx int) error) error {
	// Find the first key with the prefix.
	firstMatch, firstIndex, err := r.SearchIndexEntryWithPrefix(prefix, false)
	if err != nil || firstMatch == nil {
		// return nil if none found
		return err
	}

	// Emit the first key.
	if err := cb(firstMatch, firstIndex); err != nil {
		return err
	}

	// Iterate until the prefix no longer matches.
	size := int(r.Size())
	for i := firstIndex + 1; i < size; i++ {
		indexEntry, err := r.ReadIndexEntry(uint64(i))
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(indexEntry.GetKey(), prefix) {
			break
		}
		if err := cb(indexEntry, i); err != nil {
			return err
		}
	}

	return nil
}

// ScanPrefixKeys iterates over keys with a prefix.
func (r *Reader) ScanPrefixKeys(prefix []byte, cb func(key []byte) error) error {
	return r.ScanPrefixEntries(prefix, func(indexEntry *IndexEntry, indexEntryIdx int) error {
		return cb(indexEntry.GetKey())
	})
}

// ScanPrefix iterates over key/value pairs with a prefix.
func (r *Reader) ScanPrefix(prefix []byte, cb func(key, value []byte) error) error {
	return r.ScanPrefixEntries(prefix, func(indexEntry *IndexEntry, indexEntryIdx int) error {
		data, err := r.GetWithEntry(indexEntry, indexEntryIdx)
		if err != nil {
			return err
		}
		return cb(indexEntry.GetKey(), data)
	})
}

// GetValueSize looks up the size of the value for the given key without reading the value.
// Returns -1, nil if not found.
func (r *Reader) GetValueSize(key []byte) (int64, error) {
	valueIdx, valueLen, _, _, err := r.GetValuePosition(key)
	if err != nil || valueLen < 0 || valueIdx < 0 {
		return -1, err
	}
	return valueLen, nil
}
