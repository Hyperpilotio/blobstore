package blobstore

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

var blobStoreConfig *viper.Viper

type TestDeployment struct {
	Name string
	Type string
}

const (
	testJsonFilePath = "/home/alan/test-13274590fb39.json"
	testProjectId    = "test-179902"
	testKind         = "testStore"
)

func init() {
	viper := viper.New()
	viper.SetConfigType("json")
	viper.Set("gpcServiceAccountJSONFile", testJsonFilePath)
	blobStoreConfig = viper
}

func TestGCPDatastore(t *testing.T) {
	datastoreDB, err := NewDatastoreDB(testKind, blobStoreConfig)
	if err != nil {
		panic(err)
	}

	// Store
	deployment := &TestDeployment{
		Name: "redis",
		Type: "GCP",
	}
	err = datastoreDB.Store(deployment.Name, deployment)
	assert.Nil(t, err, "Datastore store error should be nil")

	// LoadAll
	testDeployments, err := datastoreDB.LoadAll(func() interface{} {
		return &TestDeployment{}
	})
	assert.Nil(t, err, "Datastore loadAll error should be nil")
	assert.Equal(t, deployment.Name, testDeployments.([]interface{})[0].(*TestDeployment).Name)

	// Load
	testDeployment := &TestDeployment{}
	err = datastoreDB.Load(deployment.Name, testDeployment)
	assert.Nil(t, err, "Datastore load error should be nil")
	assert.Equal(t, deployment.Name, testDeployment.Name)

	// Delete
	err = datastoreDB.Delete(deployment.Name)
	assert.Nil(t, err, "Datastore delete error should be nil")
}
