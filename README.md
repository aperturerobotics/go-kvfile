# Key-Value File (kvfile)

[![Go Reference](https://pkg.go.dev/badge/github.com/aperturerobotics/go-kvfile.svg)](https://pkg.go.dev/github.com/aperturerobotics/go-kvfile)
[![Go Report Card Widget]][Go Report Card]

[Go Report Card Widget]: https://goreportcard.com/badge/github.com/aperturerobotics/go-kvfile
[Go Report Card]: https://goreportcard.com/report/github.com/aperturerobotics/go-kvfile

## Introduction

**go-kvfile** stores key/value pairs to a file with `O(log N)` (binary search) lookup.

The values are concatenated together at the beginning of the file, followed by a
set of length-suffixed entries containing each key and the offset of the
associated value, followed by a list of positions of index entries.

The [compress](./compress) package supports seekable-zstd compressed kvfiles.

## CLI

The kvfile CLI can be used to read/write a kvfile on the command line:

```
NAME:
   kvfile - A CLI tool for working with key-value files

COMMANDS:
   count    Print the number of keys in a k/v file.
   keys     Print all keys in a k/v file in sorted order.
   values   Print all key-value pairs in a k/v file.
   get      Get the value for a specific key.
   write    Write a new kvfile from JSON input.

GLOBAL OPTIONS:
   --binary-keys           read and log keys as binary (base58) (default: false)
   --binary-values         read and log values as binary (base58) (default: true)
   --file value, -f value  path to the kvfile to read
   --compress              use kvfile compression (default: false)
```

## Usage

The Reader reads values from the kvfile and can search for specific keys using a
binary search on the key index:

```
// Read keys from the file.
Get(): Looks up the value for the given key.
ReadTo(): Reads the value for the given key to the writer.
Exists(): Checks if the given key exists in the store.

// Iterate over keys in the file.
ScanPrefix(): iterates over key/value pairs with a prefix.
ScanPrefixKeys(): iterates over key/value pairs with a prefix, returning keys only.

// Utilities for reading the file structure.
ReadIndexEntry(): Reads the index entry at the given index.
SearchIndexEntry(): Looks up an index entry for the given key.
GetValuePosition(): Determines the position and length of the value for the key.
```

Write() writes the given key-value pairs to the file with the writer.

```go
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
		return uint64(nw), nil
	})
```

The Writer can be used to incrementally write keys and values to a file.

```go
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

	wr := NewWriter(&buf)
	for i, key := range keys {
		if err := wr.WriteValue(key, bytes.NewReader(vals[i])); err != nil {
			t.Fatal(err.Error())
		}
	}

	if err := wr.Close(); err != nil {
		t.Fatal(err.Error())
	}
```

## Support

Please open a [GitHub issue] with any questions / issues.

[GitHub issue]: https://github.com/aperturerobotics/go-kvfile/issues/new

... or feel free to reach out on [Matrix Chat] or [Discord].

[Discord]: https://discord.gg/KJutMESRsT
[Matrix Chat]: https://matrix.to/#/#aperturerobotics:matrix.org

## License

MIT

