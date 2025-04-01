package kvfile

import "errors"

// Predefined errors for the kvfile package.
var (
	// ErrFileSizeTooSmallForIndexCount indicates the file size is too small to contain the index count.
	ErrFileSizeTooSmallForIndexCount = errors.New("file size too small for index count")
	// ErrMaxIndexEntrySizeNegative indicates the configured maxIndexEntrySize is negative.
	ErrMaxIndexEntrySizeNegative = errors.New("maxIndexEntrySize is negative")
	// ErrBufferCapacityTooSmall indicates an internal buffer had insufficient capacity.
	ErrBufferCapacityTooSmall = errors.New("buffer capacity less than 10")
	// ErrNegativeIndexBinarySearch indicates a negative index was calculated during binary search.
	ErrNegativeIndexBinarySearch = errors.New("negative index calculated in binary search")
	// ErrNegativeIndexCalculated indicates a negative index was calculated.
	ErrNegativeIndexCalculated = errors.New("negative index calculated")
	// ErrEntryValueNotFound indicates the value for a given index entry could not be located.
	ErrEntryValueNotFound = errors.New("entry value not found")
	// ErrNegativeIndexScan indicates a negative index was encountered during a scan operation.
	ErrNegativeIndexScan = errors.New("negative index in scan")
)
