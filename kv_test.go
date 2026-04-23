package kvfile

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/pkg/errors"
)

func TestKvStore(t *testing.T) {
	var buf bytes.Buffer
	keys := [][]byte{
		[]byte("test-1"),
		[]byte("test-2"),
		[]byte("test-3"),
	}
	vals := [][]byte{
		[]byte("val-1"),
		[]byte("val-2"),
		[]byte("val-3"),
	}
	// we write the keys in sequential order, use that here:
	var index int
	err := Write(&buf, keys, func(wr io.Writer, key []byte) (uint64, error) {
		nw, err := wr.Write(vals[index])
		if err != nil {
			return 0, err
		}
		index++
		return uint64(nw), nil //nolint:gosec
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	bufReader := bytes.NewReader(buf.Bytes())
	rdr, err := BuildReader(bufReader, uint64(buf.Len())) //nolint:gosec
	if err != nil {
		t.Fatal(err.Error())
	}

	keyExists, err := rdr.Exists(keys[0])
	if err != nil {
		t.Fatal(err.Error())
	}
	if !keyExists {
		t.Fatalf("expected key to exist: %s", string(keys[0]))
	}

	keyExists, err = rdr.Exists([]byte("does-not-exist"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if keyExists {
		t.Fatal("expected key to not exist")
	}

	for i := range keys {
		data, found, err := rdr.Get(keys[i])
		if err != nil {
			t.Fatal(err.Error())
		}
		if !found {
			t.Fatalf("expected key to exist: %s", string(keys[i]))
		}
		if !bytes.Equal(data, vals[i]) {
			t.Fatalf("value mismatch %s: got %v expected %v", string(keys[i]), data, vals[i])
		}
	}

	prefixEntry, prefixIdx, err := rdr.SearchIndexEntryWithPrefix([]byte("test-"), false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 0 || !bytes.Equal(prefixEntry.GetKey(), []byte("test-1")) {
		t.Fatalf("search prefix last=false failed: %v %v", prefixIdx, string(prefixEntry.GetKey()))
	}

	data, err := rdr.GetWithEntry(prefixEntry, prefixIdx)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !bytes.Equal(data, vals[0]) {
		t.FailNow()
	}

	prefixEntry, prefixIdx, err = rdr.SearchIndexEntryWithPrefix([]byte("test-"), true)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 2 || !bytes.Equal(prefixEntry.GetKey(), []byte("test-3")) {
		t.FailNow()
	}

	data, err = rdr.GetWithEntry(prefixEntry, prefixIdx)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !bytes.Equal(data, vals[2]) {
		t.FailNow()
	}

	var seenVals int
	err = rdr.ScanPrefix([]byte("test-"), func(key, value []byte) error {
		seenVals++
		if string(key) != "test-"+strconv.Itoa(seenVals) {
			return errors.Errorf("unexpected key: %s", string(key))
		}
		if !bytes.Equal(value, vals[seenVals-1]) {
			return errors.Errorf("unexpected value for %s: %v", string(key), value)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	prefixEntry, prefixIdx, err = rdr.SearchIndexEntryWithPrefix([]byte("test-2b"), false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 2 || prefixEntry != nil {
		t.FailNow()
	}

	prefixEntry, prefixIdx, err = rdr.SearchIndexEntryWithPrefix([]byte("test-2b"), true)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 2 || prefixEntry != nil {
		t.FailNow()
	}

	// . is the next char after -
	prefixEntry, prefixIdx, err = rdr.SearchIndexEntryWithPrefix([]byte("test."), false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 3 || prefixEntry != nil {
		t.FailNow()
	}

	// . is the next char after -
	prefixEntry, prefixIdx, err = rdr.SearchIndexEntryWithPrefix([]byte("test."), true)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 3 || prefixEntry != nil {
		t.FailNow()
	}

	prefixEntry, prefixIdx, err = rdr.SearchIndexEntryWithPrefix([]byte("test-1b"), false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 1 || prefixEntry != nil {
		t.FailNow()
	}
}

func TestKvStoreEmpty(t *testing.T) {
	var buf bytes.Buffer
	err := Write(&buf, nil, nil)
	if err != nil {
		t.Fatal(err.Error())
	}

	bufReader := bytes.NewReader(buf.Bytes())
	rdr, err := BuildReader(bufReader, uint64(buf.Len())) //nolint:gosec
	if err != nil {
		t.Fatal(err.Error())
	}

	ent, err := rdr.ReadIndexEntry(0)
	if err == nil || ent != nil {
		t.FailNow()
	}

	// verify that no keys exist
	keyExists, err := rdr.Exists([]byte("test"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if keyExists {
		t.Fatal("expected no keys to exist")
	}

	err = rdr.ScanPrefix(nil, func(key, value []byte) error {
		return errors.New("expected no keys to exist")
	})
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestIndexTailReader(t *testing.T) {
	var buf bytes.Buffer
	keys := [][]byte{
		[]byte("test-1"),
		[]byte("test-2"),
		[]byte("test-3"),
	}
	vals := [][]byte{
		[]byte("val-1"),
		[]byte("val-2"),
		[]byte("val-3"),
	}
	var index int
	err := Write(&buf, keys, func(wr io.Writer, key []byte) (uint64, error) {
		nw, err := wr.Write(vals[index])
		if err != nil {
			return 0, err
		}
		index++
		return uint64(nw), nil //nolint:gosec
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	fileSize := uint64(buf.Len()) //nolint:gosec
	full := bytes.NewReader(buf.Bytes())
	tailStart, tail, err := ReadIndexTail(full, fileSize)
	if err != nil {
		t.Fatal(err.Error())
	}
	if tailStart == 0 || len(tail) == 0 {
		t.Fatalf("expected non-empty tail after values, got start=%d len=%d", tailStart, len(tail))
	}
	if len(tail) >= buf.Len() {
		t.Fatalf("tail length %d should be smaller than file length %d", len(tail), buf.Len())
	}
	trimStart, trimmed, err := TrimIndexTail(buf.Bytes()[tailStart-1:], fileSize)
	if err != nil {
		t.Fatal(err.Error())
	}
	if trimStart != tailStart {
		t.Fatalf("trimmed tail start = %d, want %d", trimStart, tailStart)
	}
	if !bytes.Equal(trimmed, tail) {
		t.Fatal("trimmed suffix did not match exact tail")
	}

	rdr, err := BuildReaderWithIndexTail(tail, fileSize)
	if err != nil {
		t.Fatal(err.Error())
	}
	if rdr.Size() != uint64(len(keys)) {
		t.Fatalf("tail reader size = %d, want %d", rdr.Size(), len(keys))
	}
	for i, key := range keys {
		entry, idx, err := rdr.SearchIndexEntryWithKey(key)
		if err != nil {
			t.Fatal(err.Error())
		}
		if entry == nil {
			t.Fatalf("expected key %s in tail index", string(key))
		}
		if idx != i {
			t.Fatalf("index for %s = %d, want %d", string(key), idx, i)
		}
		if entry.GetOffset() >= tailStart {
			t.Fatalf("entry %s offset %d should point before tail start %d", string(key), entry.GetOffset(), tailStart)
		}
	}

	if _, _, err := rdr.Get(keys[0]); err == nil {
		t.Fatal("expected value read from tail-only reader to fail")
	}
}

func TestIndexTailReaderEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, nil, nil); err != nil {
		t.Fatal(err.Error())
	}
	fileSize := uint64(buf.Len()) //nolint:gosec
	tailStart, tail, err := ReadIndexTail(bytes.NewReader(buf.Bytes()), fileSize)
	if err != nil {
		t.Fatal(err.Error())
	}
	if tailStart != 0 {
		t.Fatalf("empty tail start = %d, want 0", tailStart)
	}
	rdr, err := BuildReaderWithIndexTail(tail, fileSize)
	if err != nil {
		t.Fatal(err.Error())
	}
	if rdr.Size() != 0 {
		t.Fatalf("empty tail reader size = %d, want 0", rdr.Size())
	}
}

func TestMaxIndexTailSize(t *testing.T) {
	size, err := MaxIndexTailSize(3)
	if err != nil {
		t.Fatal(err.Error())
	}
	want := uint64(8 + 3*(maxIndexEntrySize+10+8))
	if size != want {
		t.Fatalf("MaxIndexTailSize = %d, want %d", size, want)
	}
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	keys := [][]byte{
		[]byte("test-2"),
		[]byte("test-3"),
		[]byte("test-1"),
	}
	vals := [][]byte{
		[]byte("val-2"),
		[]byte("val-3"),
		[]byte("val-1"),
	}

	// we write the keys in the above order, use that here:
	wr := NewWriter(&buf)
	for i, key := range keys {
		if err := wr.WriteValue(key, bytes.NewReader(vals[i])); err != nil {
			t.Fatal(err.Error())
		}
	}

	if err := wr.Close(); err != nil {
		t.Fatal(err.Error())
	}

	bufReader := bytes.NewReader(buf.Bytes())
	rdr, err := BuildReader(bufReader, uint64(buf.Len())) //nolint:gosec
	if err != nil {
		t.Fatal(err.Error())
	}

	keyExists, err := rdr.Exists(keys[0])
	if err != nil {
		t.Fatal(err.Error())
	}
	if !keyExists {
		t.Fatalf("expected key to exist: %s", string(keys[0]))
	}

	keyExists, err = rdr.Exists([]byte("does-not-exist"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if keyExists {
		t.Fatal("expected key to not exist")
	}

	for i, key := range keys {
		data, found, err := rdr.Get(key)
		if err != nil {
			t.Fatal(err.Error())
		}
		if !found {
			t.Fatalf("expected key to exist: %s", string(keys[i]))
		}
		if !bytes.Equal(data, vals[i]) {
			t.Fatalf("value mismatch %s: got %v expected %v", string(keys[i]), data, vals[i])
		}
	}

	prefixEntry, prefixIdx, err := rdr.SearchIndexEntryWithPrefix([]byte("test-"), false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if prefixIdx != 0 || !bytes.Equal(prefixEntry.GetKey(), []byte("test-1")) {
		t.Fatalf("search prefix last=false failed: %v %v", prefixIdx, string(prefixEntry.GetKey()))
	}
}
