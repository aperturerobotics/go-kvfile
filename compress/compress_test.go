package kvfile_compress

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestKvCompress(t *testing.T) {
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
	err := WriteCompress(&buf, keys, func(wr io.Writer, key []byte) (uint64, error) {
		nw, err := wr.Write(vals[index])
		if err != nil {
			return 0, err
		}
		// Check non-negative before conversion
		if nw < 0 {
			return 0, errors.New("writer returned negative bytes written")
		}
		index++
		return uint64(nw), nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	bufReader := bytes.NewReader(buf.Bytes())
	rdr, rdrRelease, err := BuildCompressReader(bufReader)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer rdrRelease()

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
}
