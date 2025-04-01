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
	if fileSize < 8 {
		return nil, ErrFileSizeTooSmallForIndexCount
	}
	// Check for overflow before converting fileSize to int64
	if fileSize > math.MaxInt64 {
		return nil, errors.Errorf("file size %v overflows int64", fileSize)
	}
	indexEntryCountPos := int64(fileSize) - 8
	buf := make([]byte, 8)
	_, err := rd.ReadAt(buf, indexEntryCountPos)
	if err != nil {
		return nil, err
	}
	indexEntryCount := binary.LittleEndian.Uint64(buf)
	// Check for overflow before multiplication and before converting indexEntryCountPos
	if indexEntryCount > math.MaxUint64/8 {
		return nil, errors.Errorf("index entry count %v too large, would overflow multiplication", indexEntryCount)
	}
	indexEntriesTotalSize := indexEntryCount * 8
	if indexEntryCountPos < 0 {
		// This should not happen if fileSize >= 8, but check defensively.
		return nil, errors.Errorf("indexEntryCountPos %v is negative", indexEntryCountPos)
	}
	if indexEntriesTotalSize > uint64(indexEntryCountPos) {
		return nil, errors.Errorf("index entry count %v too large for file size", indexEntryCount)
	}
	// Check for overflow before subtraction
	if uint64(indexEntryCountPos) < indexEntriesTotalSize {
		// Should be caught by the previous check, but check defensively.
		return nil, errors.Errorf("indexEntryCountPos %v is smaller than index entries total size %v", indexEntryCountPos, indexEntriesTotalSize)
	}
	indexEntryIndexesPos := uint64(indexEntryCountPos) - indexEntriesTotalSize
	// indexEntryIndexesPos is now uint64, no need for negative check.
	// clear the buf
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	// read the first index entry pos
	if indexEntryIndexesPos > uint64(math.MaxInt64) {
		return nil, errors.Errorf("index entry indexes position %v overflows int64", indexEntryIndexesPos)
	}
	_, err = rd.ReadAt(buf, int64(indexEntryIndexesPos))
	if err != nil {
		return nil, err
	}
	firstIndexEntryLenPos := binary.LittleEndian.Uint64(buf)
	// read the size of the first index entry
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	if firstIndexEntryLenPos > uint64(math.MaxInt64) {
		return nil, errors.Errorf("first index entry length position %v overflows int64", firstIndexEntryLenPos)
	}
	_, err = rd.ReadAt(buf, int64(firstIndexEntryLenPos))
	if err != nil {
		return nil, err
	}
	indexEntrySize, indexEntrySizeLen := protobuf_go_lite.ConsumeVarint(buf)
	if indexEntrySizeLen < 0 {
		return nil, errors.Errorf("invalid index entry size varint at %v", firstIndexEntryLenPos)
	}
	// maxIndexEntrySize is int, check non-negative before conversion (though it's constant positive)
	if maxIndexEntrySize < 0 {
		return nil, ErrMaxIndexEntrySizeNegative
	}
	if indexEntrySize > uint64(maxIndexEntrySize) {
		return nil, errors.Errorf("invalid index entry size at %v: %v > %v", firstIndexEntryLenPos, indexEntrySize, maxIndexEntrySize)
	}
	// determine the position of the first IndexEntry entry
	// Check for underflow before subtraction
	if firstIndexEntryLenPos < indexEntrySize {
		return nil, errors.Errorf("invalid index entry size %v at %v: larger than its position", indexEntrySize, firstIndexEntryLenPos)
	}
	indexEntryListPos := firstIndexEntryLenPos - indexEntrySize
	// Check for overflow before converting indexEntryListPos to int64 (though it's not used as int64 later)
	// Check for overflow before converting indexEntryIndexesPos back to uint64 (already uint64)
	return &Reader{
		rd:                   rd,
		indexEntryCount:      indexEntryCount,
		indexEntryIndexesPos: indexEntryIndexesPos,
		indexEntryListPos:    indexEntryListPos,
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
	if size < 0 {
		return nil, errors.Errorf("file size %v cannot be negative", size)
	}
	return BuildReader(f, uint64(size))
}

// ReadIndexEntry reads the index entry at the given index.
func (r *Reader) ReadIndexEntry(indexEntryIdx uint64) (*IndexEntry, error) {
	if indexEntryIdx >= r.indexEntryCount {
		return nil, errors.Errorf("out-of-bounds read of index entry: %v > %v", indexEntryIdx, r.indexEntryCount)
	}
	// determine the position of the entry in the positions list
	// Check for overflow before multiplication and addition
	if indexEntryIdx > (math.MaxUint64-r.indexEntryIndexesPos)/8 {
		return nil, errors.Errorf("index entry index %v too large, would overflow calculation", indexEntryIdx)
	}
	indexEntryLocPos := r.indexEntryIndexesPos + (8 * indexEntryIdx)
	// read the entry position
	buf := make([]byte, 8, 10)
	if indexEntryLocPos > uint64(math.MaxInt64) {
		return nil, errors.Errorf("index entry location position %v overflows int64", indexEntryLocPos)
	}
	_, err := r.rd.ReadAt(buf, int64(indexEntryLocPos))
	if err != nil {
		return nil, err
	}
	// determine the position of the index entry size varint
	indexEntrySizePos := binary.LittleEndian.Uint64(buf)
	// read the index entry size varint
	// Ensure buffer has capacity before slicing
	if cap(buf) < 10 {
		// This should not happen with make([]byte, 8, 10)
		return nil, ErrBufferCapacityTooSmall
	}
	buf = buf[:10] //nolint:gosec
	for i := range buf {
		buf[i] = 0
	}
	if indexEntrySizePos > uint64(math.MaxInt64) {
		return nil, errors.Errorf("index entry size position %v overflows int64", indexEntrySizePos)
	}
	_, err = r.rd.ReadAt(buf, int64(indexEntrySizePos))
	if err != nil {
		return nil, err
	}
	indexEntrySize, indexEntrySizeLen := protobuf_go_lite.ConsumeVarint(buf)
	if indexEntrySizeLen < 0 {
		return nil, errors.Errorf("invalid index entry size varint at %v", indexEntrySizePos)
	}
	// maxIndexEntrySize is int, check non-negative before conversion (though it's constant positive)
	if maxIndexEntrySize < 0 {
		return nil, ErrMaxIndexEntrySizeNegative
	}
	if indexEntrySize > uint64(maxIndexEntrySize) {
		return nil, errors.Errorf("invalid index entry size at %v: %v > %v", indexEntrySizePos, indexEntrySize, maxIndexEntrySize)
	}
	// Check for underflow before subtraction
	if indexEntrySizePos < indexEntrySize {
		return nil, errors.Errorf("invalid index entry size %v at %v: larger than its position", indexEntrySize, indexEntrySizePos)
	}
	indexEntryPos := indexEntrySizePos - indexEntrySize
	// Check size before allocating buffer
	if indexEntrySize > uint64(math.MaxInt) {
		// Prevent allocating overly large buffer potentially leading to OOM
		return nil, errors.Errorf("index entry size %v too large to allocate buffer", indexEntrySize)
	}
	buf = make([]byte, indexEntrySize)
	if indexEntryPos > uint64(math.MaxInt64) {
		return nil, errors.Errorf("index entry position %v overflows int64", indexEntryPos)
	}
	_, err = r.rd.ReadAt(buf, int64(indexEntryPos))
	if err != nil {
		return nil, err
	}
	indexEntry := &IndexEntry{}
	if err := indexEntry.UnmarshalVT(buf); err != nil {
		return nil, errors.Wrapf(err, "invalid index entry at %v", indexEntryPos)
	}
	// Check for potential negative indexEntryPos before converting to uint64 (already uint64)
	if off := indexEntry.GetOffset(); off > indexEntryPos {
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
	// Check for overflow before converting indexEntryCount to int
	if r.indexEntryCount > uint64(math.MaxInt) {
		return nil, -1, errors.Errorf("index entry count %v overflows int", r.indexEntryCount)
	}
	i, j := 0, int(r.indexEntryCount)
	for i < j {
		// Calculate midpoint avoiding potential overflow of i+j
		h := i + (j-i)/2

		// Check non-negative before converting h to uint64 (should always be true if i, j >= 0)
		if h < 0 {
			return nil, h, ErrNegativeIndexBinarySearch
		}

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
		// Check for overflow before converting indexEntryCount to int
		if r.indexEntryCount > uint64(math.MaxInt) {
			return nil, -1, errors.Errorf("index entry count %v overflows int", r.indexEntryCount)
		}
		count := int(r.indexEntryCount)
		if last {
			if count == 0 {
				return nil, -1, nil // Empty set
			}
			idx = count - 1
		}
		if idx >= 0 && idx < count {
			// Check non-negative before converting idx to uint64
			if idx < 0 {
				return nil, idx, ErrNegativeIndexCalculated
			}
			ent, err := r.ReadIndexEntry(uint64(idx))
			return ent, idx, err
		}
		return nil, idx, nil
	}

	// Check for overflow before converting indexEntryCount to int
	if r.indexEntryCount > uint64(math.MaxInt) {
		return nil, -1, errors.Errorf("index entry count %v overflows int", r.indexEntryCount)
	}
	count := int(r.indexEntryCount)
	if count == 0 {
		return nil, 0, nil // Not found in empty set, insertion point 0
	}
	i, j := 0, count-1
	var matchedEntry *IndexEntry
	var matchedIdx int = -1 // Initialize to indicate no match found yet
	for i <= j {
		// Calculate midpoint avoiding potential overflow of i+j
		h := i + (j-i)/2

		// Check non-negative before converting h to uint64 (should always be true if i, j >= 0)
		if h < 0 {
			return nil, h, ErrNegativeIndexBinarySearch
		}
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
// Returns -1, -1, error if invalid.
func (r *Reader) GetValuePositionWithEntry(indexEntry *IndexEntry, indexEntryIdx int) (idx, length int64, err error) {
	offset := indexEntry.GetOffset()
	size := indexEntry.GetSize()

	// Check for overflow before converting to int64
	if offset > uint64(math.MaxInt64) {
		return -1, -1, errors.Errorf("value offset %v overflows int64", offset)
	}
	if size > uint64(math.MaxInt64) {
		return -1, -1, errors.Errorf("value size %v overflows int64", size)
	}
	valueOffset := int64(offset)
	valueSize := int64(size)

	// Check against configured max value size
	if size > uint64(maxValueSize) {
		return -1, -1, errors.Errorf("value size %v > max size %v", size, maxValueSize)
	}

	// Check for overflow before addition
	if valueOffset > math.MaxInt64-valueSize {
		return -1, -1, errors.Errorf("value offset %v + size %v overflows int64", valueOffset, valueSize)
	}
	valueEnd := valueOffset + valueSize

	// Check bounds against the start of the index entries positions list
	if r.indexEntryIndexesPos > uint64(math.MaxInt64) {
		// This should ideally be checked earlier, but double-check here.
		return -1, -1, errors.Errorf("index entry indexes position %v overflows int64", r.indexEntryIndexesPos)
	}
	if valueEnd > int64(r.indexEntryIndexesPos) {
		return -1, -1, errors.Errorf("value end position %v is beyond index start %v", valueEnd, r.indexEntryIndexesPos)
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
		err = ErrEntryValueNotFound
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
	if err != nil {
		return err // Propagate error from search
	}
	if firstMatch == nil {
		return nil // No entries with the prefix found
	}

	// Emit the first key.
	if err := cb(firstMatch, firstIndex); err != nil {
		return err
	}

	// Iterate until the prefix no longer matches.
	sizeUint64 := r.Size()
	// Check for overflow before converting Size to int
	if sizeUint64 > uint64(math.MaxInt) {
		return errors.Errorf("index entry count %v overflows int", sizeUint64)
	}
	size := int(sizeUint64)
	for i := firstIndex + 1; i < size; i++ {
		// Check non-negative before converting i to uint64 (should always be true if loop starts >= 0)
		if i < 0 {
			return ErrNegativeIndexScan
		}
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
