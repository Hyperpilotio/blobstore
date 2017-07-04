package blobstore

import (
	"errors"
	"strings"
)

type BlobStore interface {
	Store(key string, object interface{}) error
	// Ugly way to abstract the return type, as in Go you can't cast []interface{} to a
	// specific type. Users will have to assume the interface{} is a array type of
	// objects created by the factory func.
	// Factory is the function to create a per typed interface object
	LoadAll(factory func() interface{}) (interface{}, error)
	Delete(key string) error
}

type BlobStoreConfig interface {
	GetString(name string) string
}

func NewBlobStore(name string, config BlobStoreConfig) (BlobStore, error) {
	storeType := strings.ToLower(config.GetString("store.type"))
	switch storeType {
	case "simpledb":
		return NewSimpleDB(name, config)
	case "file":
		return NewFile(name, config)
	default:
		return nil, errors.New("Unsupported store type: " + storeType)
	}
}
