package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/aperturerobotics/go-kvfile"
	kvfile_compress "github.com/aperturerobotics/go-kvfile/compress"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

var (
	filePath       string
	binKeys        bool
	binValues      bool = true
	readCompressed bool
	valueStr       string
	keyStr         string
)

func main() {
	app := &cli.App{
		Name:  "kvfile",
		Usage: "A CLI tool for working with key-value files",
		Authors: []*cli.Author{
			{Name: "Christian Stewart", Email: "christian@aperture.us"},
		},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "binary-keys",
				Usage:       "read and log keys as binary (base58)",
				Value:       binKeys,
				Destination: &binKeys,
			},
			&cli.BoolFlag{
				Name:        "binary-values",
				Usage:       "read and log values as binary (base58)",
				Value:       binValues,
				Destination: &binValues,
			},
			&cli.StringFlag{
				Name:        "file",
				Usage:       "path to the kvfile to read",
				Aliases:     []string{"f"},
				Value:       filePath,
				Destination: &filePath,
			},
			&cli.BoolFlag{
				Name:        "compress",
				Usage:       "use kvfile compression",
				Value:       readCompressed,
				Destination: &readCompressed,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "count",
				Usage: "Print the number of keys in a k/v file.",
				Action: func(c *cli.Context) error {
					reader, rel, err := openKVFile(filePath)
					if rel != nil {
						defer rel()
					}
					if err != nil {
						return err
					}

					numKeys := reader.Size()
					fmt.Printf("%d\n", numKeys)
					return nil
				},
			},
			{
				Name:  "keys",
				Usage: "Print all keys in a k/v file in sorted order.",
				Action: func(c *cli.Context) error {
					reader, rel, err := openKVFile(filePath)
					if rel != nil {
						defer rel()
					}
					if err != nil {
						return err
					}

					return iterateAndPrintKeys(reader)
				},
			},
			{
				Name:  "values",
				Usage: "Print all key-value pairs in a k/v file.",
				Action: func(c *cli.Context) error {
					reader, rel, err := openKVFile(filePath)
					if rel != nil {
						defer rel()
					}
					if err != nil {
						return err
					}

					return printAll(reader)
				},
			},
			{
				Name:  "get",
				Usage: "Get the value for a specific key.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "key",
						Usage:       "the key to look up",
						Value:       keyStr,
						Destination: &keyStr,
					},
				},
				Action: func(c *cli.Context) error {
					if keyStr == "" {
						return fmt.Errorf("please provide a key")
					}

					reader, rel, err := openKVFile(filePath)
					if rel != nil {
						defer rel()
					}
					if err != nil {
						return err
					}

					val, found, err := reader.Get([]byte(keyStr))
					if err != nil {
						return err
					}
					if !found {
						return errors.Errorf("Key %q not found.\n", keyStr)
					}
					printData(val, binValues)
					return nil
				},
			},
			{
				Name:  "write",
				Usage: "Write a new kvfile from JSON input.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "json",
						Usage:       "the JSON data to write",
						Value:       valueStr,
						Destination: &valueStr,
					},
				},
				Action: func(c *cli.Context) error {
					if filePath == "" {
						return fmt.Errorf("please provide a file path")
					}
					if valueStr == "" {
						return fmt.Errorf("please provide JSON data to write")
					}

					var data map[string]string
					err := json.Unmarshal([]byte(valueStr), &data)
					if err != nil {
						return err
					}

					file, err := os.Create(filePath)
					if err != nil {
						return err
					}
					defer file.Close()

					keys := make([][]byte, 0, len(data))
					for k := range data {
						keys = append(keys, []byte(k))
					}

					return kvfile.Write(file, keys, func(wr io.Writer, key []byte) (uint64, error) {
						val := data[string(key)]
						n, err := wr.Write([]byte(val))
						// Check non-negative before conversion
						if n < 0 {
							return 0, errors.Wrap(err, "writer returned negative bytes written")
						}
						return uint64(n), err
					})
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}

// shared helper to open kvfile based on flags
func openKVFile(filePath string) (*kvfile.Reader, func(), error) {
	if filePath == "" {
		return nil, nil, errors.New("please provide a file path")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}

	if readCompressed {
		reader, readerRel, err := kvfile_compress.BuildCompressReader(file)
		if err != nil {
			_ = file.Close()
			return nil, nil, err
		}
		return reader, func() {
			readerRel()
			_ = file.Close()
		}, nil
	}

	reader, err := kvfile.BuildReaderWithFile(file)
	return reader, nil, err
}

func iterateAndPrintKeys(reader *kvfile.Reader) error {
	size := reader.Size()
	for i := uint64(0); i < size; i++ {
		indexEntry, err := reader.ReadIndexEntry(i)
		if err != nil {
			return err
		}
		printData(indexEntry.GetKey(), binKeys)
	}
	return nil
}

func printData(key []byte, bin bool) {
	var output string
	if bin {
		output = b58.Encode(key)
	} else {
		output = string(key)
	}
	os.Stdout.WriteString(output + "\n")
}

func printAll(reader *kvfile.Reader) error {
	size := reader.Size()
	if size == 0 {
		fmt.Println("No key-value pairs found.")
		return nil
	}

	for i := uint64(0); i < size; i++ {
		indexEntry, err := reader.ReadIndexEntry(i)
		if err != nil {
			return err
		}
		key := indexEntry.GetKey()
		printData(key, binKeys)

		// Check for overflow before converting i to int
		if i > uint64(math.MaxInt) {
			return errors.Errorf("key index %v overflows int", i)
		}
		val, err := reader.GetWithEntry(indexEntry, int(i))
		if err != nil {
			return err
		}
		printData(val, binValues)
	}

	return nil
}
