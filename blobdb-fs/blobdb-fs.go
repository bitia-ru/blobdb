package blobdb_fs

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"github.com/bitia-ru/blobdb/blobdb"
	"io"
	"os"
	"path/filepath"
)

const DB_NESTING_LEVELS = 3

func randomString(n int) (string, error) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	for i := range b {
		b[i] = letters[b[i]%byte(len(letters))]
	}

	return string(b), nil
}

type Object struct {
	dbDirPath string
	fileInfo  os.FileInfo
	hashSum   string
}

func (object *Object) Open() (io.ReadCloser, error) {
	file, err := os.Open(filepath.Join(object.dbDirPath, object.hashSum, "blob"))

	if err != nil {
		return nil, err
	}

	return file, nil
}

func (object *Object) Size() int64 {
	return object.fileInfo.Size()
}

func (object *Object) Hash() string {
	return object.hashSum
}

type Db struct {
	dbDirPath string
}

func Open(dbDirPath string) (blobdb.Db, error) {
	fileInfo, err := os.Stat(dbDirPath)

	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dbDirPath, 0700)

			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	if !fileInfo.IsDir() {
		return nil, os.ErrInvalid
	}

	return &Db{
		dbDirPath: dbDirPath,
	}, nil
}

func (db *Db) Get(hash string) (blobdb.Object, error) {
	var parts []string

	parts = append(parts, db.dbDirPath)

	for i := 0; i < DB_NESTING_LEVELS; i++ {
		parts = append(parts, hash[i*2:i*2+2])
	}

	parts = append(parts, hash)

	objectDirPath := filepath.Join(parts...)

	objectFilePath := filepath.Join(objectDirPath, "blob")

	objectFileInfo, err := os.Stat(objectFilePath)

	if err != nil {
		return nil, err
	}

	file, err := os.Open(objectFilePath)

	if err != nil {
		return nil, err
	}

	_ = file.Close()

	return &Object{
		dbDirPath: db.dbDirPath,
		fileInfo:  objectFileInfo,
		hashSum:   hash,
	}, nil
}

func (db *Db) Put(object io.Reader) (blobdb.Object, error) {
	tempFileNameBase, err := randomString(8)

	if err != nil {
		return nil, err
	}

	tempDirPath := filepath.Join(db.dbDirPath, "temp")

	_, err = os.Stat(tempDirPath)

	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(tempDirPath, 0700)

			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	tempFilePath := filepath.Join(tempDirPath, tempFileNameBase)

	tempFile, err := os.Create(tempFilePath)

	if err != nil {
		return nil, err
	}

	defer tempFile.Close()

	hasher := sha256.New()

	multiWriter := io.MultiWriter(hasher, tempFile)

	_, err = io.Copy(multiWriter, object)

	if err != nil {
		return nil, err
	}

	hashSum := hex.EncodeToString(hasher.Sum(nil))

	var parts []string

	parts = append(parts, db.dbDirPath)

	for i := 0; i < DB_NESTING_LEVELS; i++ {
		parts = append(parts, hashSum[i*2:i*2+2])
	}

	parts = append(parts, hashSum)

	objectDirPath := filepath.Join(parts...)

	err = os.MkdirAll(objectDirPath, 0700)

	if err != nil {
		return nil, err
	}

	objectFilePath := filepath.Join(objectDirPath, "blob")

	err = os.Rename(tempFilePath, objectFilePath)

	if err != nil {
		return nil, err
	}

	objectFileInfo, err := os.Stat(objectFilePath)

	if err != nil {
		return nil, err
	}

	return &Object{
		dbDirPath: db.dbDirPath,
		fileInfo:  objectFileInfo,
		hashSum:   hashSum,
	}, nil
}

func (db *Db) Delete(hash string) error {
	var parts []string

	parts = append(parts, db.dbDirPath)

	for i := 0; i < DB_NESTING_LEVELS; i++ {
		parts = append(parts, hash[i*2:i*2+2])
	}

	parts = append(parts, hash)

	objectDirPath := filepath.Join(parts...)

	err := os.RemoveAll(objectDirPath)

	if err != nil {
		return err
	}

	return nil
}
