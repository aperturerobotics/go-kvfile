# Key-value Archive File

[![Go Reference](https://pkg.go.dev/badge/github.com/aperturerobotics/go-kvfile.svg)](https://pkg.go.dev/github.com/aperturerobotics/go-kvfile)
[![Go Report Card Widget]][Go Report Card]

[Go Report Card Widget]: https://goreportcard.com/badge/github.com/aperturerobotics/go-kvfile
[Go Report Card]: https://goreportcard.com/report/github.com/aperturerobotics/go-kvfile

## Introduction

**go-kvfile** stores key/value pairs to a file.

It is useful in cases where you need a simple and efficient way to store and
retrieve key-value data to a file.

The values are concatenated together at the beginning of the file, followed by a
set of length-suffixed entries containing each key and the offset of the
associated value, followed by a list of positions of index entries.

The Reader reads values from the kvfile and can search for specific keys using a
binary search on the key index:

```
Get(): Looks up the value for the given key.
ReadTo(): Reads the value for the given key to the writer.
Exists(): Checks if the given key exists in the store.

ReadIndexEntry(): Reads the index entry at the given index.
SearchIndexEntry(): Looks up an index entry for the given key.
GetValuePosition(): Determines the position and length of the value for the key.
ScanPrefix(): iterates over key/value pairs with a prefix.
ScanPrefixKeys(): iterates over key/value pairs with a prefix, returning keys only.
```

Write() writes the given key-value pairs to the file with the writer. It sorts
the key/value pairs by key, writes the values to the file, and builds the index.

## License

MIT
