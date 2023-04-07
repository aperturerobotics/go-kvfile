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
		return uint64(nw), nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	bufReader := bytes.NewReader(buf.Bytes())
	rdr, err := BuildReader(bufReader, uint64(buf.Len()))
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

	for i := 0; i < len(keys); i++ {
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
	rdr, err := BuildReader(bufReader, uint64(buf.Len()))
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
