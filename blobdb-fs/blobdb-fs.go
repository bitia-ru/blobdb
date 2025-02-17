package blobdb_fs

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/bitia-ru/blobdb/blobdb"
	"github.com/cockroachdb/pebble"
	"io"
	"os"
	"path"
	"path/filepath"
)

const cDbNestingLevels = 3

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
	db *Db

	fileInfo os.FileInfo
	hashSum  string
}

func (object *Object) Open() (io.ReadCloser, error) {
	file, err := os.Open(filepath.Join(object.db.dbDirPath, object.hashSum, "blob"))

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

func (object *Object) AddSecondaryID(id string) error {
	return object.db.pebbleDB.Set(
		[]byte(id),
		[]byte(object.hashSum),
		pebble.Sync,
	)
}

func (object *Object) RemoveSecondaryID(_ string) error {
	panic("not implemented")

	return nil
}

type Db struct {
	dbDirPath string
	pebbleDB  *pebble.DB
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

	pebbleDD, err := pebble.Open(path.Join(dbDirPath, "pebble"), &pebble.Options{})

	if err != nil {
		return nil, err
	}

	return &Db{
		dbDirPath: dbDirPath,
		pebbleDB:  pebbleDD,
	}, nil
}

func (db *Db) Get(hash string) (blobdb.Object, error) {
	var parts []string

	parts = append(parts, db.dbDirPath)

	for i := 0; i < cDbNestingLevels; i++ {
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
		db:       db,
		fileInfo: objectFileInfo,
		hashSum:  hash,
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

	for i := 0; i < cDbNestingLevels; i++ {
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
		db:       db,
		fileInfo: objectFileInfo,
		hashSum:  hashSum,
	}, nil
}

func (db *Db) CreateEmptyFile() (f *os.File, err error) {
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

	f, err = os.Create(tempFilePath)

	return
}

func (db *Db) PutFile(f *os.File) (blobdb.Object, error) {
	err := f.Sync()

	if err != nil {
		return nil, err
	}

	_, err = f.Seek(0, 0)

	if err != nil {
		return nil, err
	}

	hasher := sha256.New()

	_, err = io.Copy(hasher, f)

	if err != nil {
		return nil, err
	}

	hashSum := hex.EncodeToString(hasher.Sum(nil))

	var parts []string

	parts = append(parts, db.dbDirPath)

	for i := 0; i < cDbNestingLevels; i++ {
		parts = append(parts, hashSum[i*2:i*2+2])
	}

	parts = append(parts, hashSum)

	objectDirPath := filepath.Join(parts...)

	err = os.MkdirAll(objectDirPath, 0700)

	if err != nil {
		return nil, err
	}

	objectFilePath := filepath.Join(objectDirPath, "blob")

	err = os.Rename(f.Name(), objectFilePath)

	if err != nil {
		return nil, err
	}

	_ = f.Close()

	objectFileInfo, err := os.Stat(objectFilePath)

	if err != nil {
		return nil, err
	}

	return &Object{
		db:       db,
		fileInfo: objectFileInfo,
		hashSum:  hashSum,
	}, nil
}

func (db *Db) Delete(hash string) error {
	var parts []string

	parts = append(parts, db.dbDirPath)

	for i := 0; i < cDbNestingLevels; i++ {
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

func (db *Db) FindBySecondaryID(id string) (blobdb.Object, error) {
	objectHash, closer, err := db.pebbleDB.Get([]byte(id))

	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, err
	}

	defer closer.Close()

	return db.Get(string(objectHash))
}
