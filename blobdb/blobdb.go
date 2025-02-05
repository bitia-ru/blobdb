package blobdb

import (
	"io"
)

type Object interface {
	Size() int64
	Hash() string

	Open() (io.ReadCloser, error)
}

type Db interface {
	Get(hash string) (Object, error)
	Put(object io.Reader) (Object, error)
	// MoveIn(filePath string) (Object, error)
	Delete(hash string) error
}
