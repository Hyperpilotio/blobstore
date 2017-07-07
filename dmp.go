package blobstore

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// Store struct to file
func WriteObjectToFile(path string, object interface{}) error {
	b, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("Unable to marshall object to json: %s", err.Error())
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Unable to open file path with %s: %s", path, err.Error())
	}
	defer file.Close()

	_, err = file.Write(b)
	if err != nil {
		return fmt.Errorf("Unable to write byte array to file: %s", err.Error())
	}

	return nil
}

// Load file to struct
func LoadFileToObject(path string, object interface{}) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("Unable to open file path with %s: %s", path, err.Error())
	}

	if err := json.Unmarshal(b, object); err != nil {
		return fmt.Errorf("Unable to decode file to struct: %s", err.Error())
	}

	return nil
}
