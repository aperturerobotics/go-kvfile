package main

import (
	"fmt"
	"os"

	"github.com/aperturerobotics/go-kvfile"
	kvfile_compress "github.com/aperturerobotics/go-kvfile/compress"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/urfave/cli/v2"
)

var filePath string
var readBinary bool
var readCompressed bool

func main() {
	app := &cli.App{
		Name:  "kvfile",
		Usage: "A CLI tool for working with key-value files",
		Commands: []*cli.Command{
			{
				Name:  "print-keys",
				Usage: "Print all keys in a k/v file in sorted order.",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        "binary",
						Usage:       "read and log keys as binary (base58)",
						Value:       readBinary,
						Destination: &readBinary,
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
						Usage:       "treat kvfile as compressed",
						Value:       readCompressed,
						Destination: &readCompressed,
					},
				},
				Action: func(c *cli.Context) error {
					if filePath == "" {
						return fmt.Errorf("please provide a file path")
					}

					file, err := os.Open(filePath)
					if err != nil {
						return err
					}
					defer file.Close()

					var reader *kvfile.Reader
					if readCompressed {
						var rel func()
						reader, rel, err = kvfile_compress.BuildCompressReader(file)
						if rel != nil {
							defer rel()
						}
					} else {
						reader, err = kvfile.BuildReaderWithFile(file)
					}
					if err != nil {
						return err
					}

					return iterateAndPrintKeys(reader)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func iterateAndPrintKeys(reader *kvfile.Reader) error {
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
		var printKey string
		if readBinary {
			printKey = b58.Encode(key)
		} else {
			printKey = string(key)
		}
		os.Stdout.WriteString(printKey + "\n")
	}
	return nil
}
