package blobstore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sync"
)

// File store saves each key value as a seperate file in the folder
// that's specified in the Path
// This is meant to be used only for local testing and usage.
type FileStore struct {
	Name  string
	Path  string
	mutex sync.Mutex
}

func NewFile(name string, config BlobStoreConfig) (*FileStore, error) {
	Path := path.Join(config.GetString("filesPath"), name)
	os.MkdirAll(Path, os.ModePerm)
	return &FileStore{
		Name: name,
		Path: Path,
	}, nil
}

func (file *FileStore) Store(key string, object interface{}) error {
	file.mutex.Lock()
	defer file.mutex.Unlock()

	filePath := path.Join(file.Path, key)
	if err := WriteObjectToFile(filePath, object); err != nil {
		return fmt.Errorf("Unable to store file: %s", err.Error())
	}

	return nil
}

func (file *FileStore) Load(key string, object interface{}) error {
	file.mutex.Lock()
	defer file.mutex.Unlock()

	if object == nil || reflect.ValueOf(object).IsNil() {
		return errors.New("Unable to load file to nil struct...")
	}

	files, filesErr := ioutil.ReadDir(file.Path)
	if filesErr != nil {
		return fmt.Errorf("Unable to read directory %s: %s", file.Path, filesErr.Error())
	}

	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}
		if fileInfo.Name() == key {
			filePath := path.Join(file.Path, fileInfo.Name())
			if err := LoadFileToObject(filePath, object); err != nil {
				return fmt.Errorf("Unable to load file %s: %s", filePath, err.Error())
			}
			break
		}
	}

	return nil
}

func (file *FileStore) LoadAll(f func() interface{}) (interface{}, error) {
	file.mutex.Lock()
	defer file.mutex.Unlock()

	items := []interface{}{}
	files, filesErr := ioutil.ReadDir(file.Path)
	if filesErr != nil {
		return nil, fmt.Errorf("Unable to read directory %s: %s", file.Path, filesErr.Error())
	}

	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}
		v := f()
		filePath := path.Join(file.Path, fileInfo.Name())
		if err := LoadFileToObject(filePath, v); err != nil {
			return nil, fmt.Errorf("Unable to load file %s: %s", filePath, err.Error())
		}
		items = append(items, v)
	}

	return items, nil
}

func (file *FileStore) Delete(key string) error {
	file.mutex.Lock()
	defer file.mutex.Unlock()

	return os.Remove(path.Join(file.Path, key))
}
