package blobdb

import (
	"io"
	"os"
)

type Object interface {
	Size() int64
	Hash() string

	Open() (io.ReadCloser, error)

	AddSecondaryID(id string) error
	RemoveSecondaryID(id string) error
}

type Db interface {
	Get(hash string) (Object, error)
	Put(object io.Reader) (Object, error)
	CreateEmptyFile() (f *os.File, err error)
	PutFile(f *os.File) (Object, error)
	Delete(hash string) error

	FindBySecondaryID(id string) (Object, error)
}
