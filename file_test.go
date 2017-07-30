package blobstore

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewFileStore(name string) (*FileStore, error) {
	storeDir, err := ioutil.TempDir("/tmp", "filestoretest")
	if err != nil {
		return nil, err
	}

	return &FileStore{
		Name: name,
		Path: storeDir,
	}, nil
}

func TestStoreLoad(t *testing.T) {
	store, err := NewFileStore("testStore")
	if err != nil {
		panic(err)
	}

	data := &struct {
		Data string
	}{Data: "testing"}
	err = store.Store("key1", data)
	assert.Nil(t, err, "Store error should be nil")

	newData := &struct {
		Data string
	}{Data: ""}

	err = store.Load("key1", newData)
	assert.Nil(t, err, "Load error should be nil")
}
