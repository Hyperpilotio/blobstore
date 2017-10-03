package blobstore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/spf13/viper"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	datastore "google.golang.org/api/datastore/v1"
)

type DatastoreDB struct {
	Name         string
	DomainName   string
	ProjectId    string
	Config       BlobStoreConfig
	datastoreSvc *datastore.Service
}

func NewDatastoreDB(name string, config BlobStoreConfig) (*DatastoreDB, error) {
	serviceAccountPath := config.GetString("gpcServiceAccountJSONFile")
	datastoreSvc, err := createDatastoreService(serviceAccountPath)
	if err != nil {
		return nil, errors.New("Unable to create GCP datastore service: " + err.Error())
	}

	domainName := getDomainName(name, config)
	projectId, err := getProjectId(serviceAccountPath)
	if err != nil {
		return nil, errors.New("Unable to get project id from service account file: " + err.Error())
	}

	return &DatastoreDB{
		Name:         name,
		DomainName:   domainName,
		ProjectId:    projectId,
		Config:       config,
		datastoreSvc: datastoreSvc,
	}, nil
}

func (db *DatastoreDB) Store(key string, object interface{}) error {
	properties := map[string]datastore.Value{}
	if err := recursiveEntityProperties(properties, object); err != nil {
		return errors.New("Unable to set properties to entity: " + err.Error())
	}

	_, err := db.datastoreSvc.Projects.
		Commit(db.ProjectId, &datastore.CommitRequest{
			Mode: "NON_TRANSACTIONAL",
			Mutations: []*datastore.Mutation{
				&datastore.Mutation{
					Insert: &datastore.Entity{
						Key: &datastore.Key{
							PartitionId: &datastore.PartitionId{
								ProjectId: db.ProjectId,
							},
							Path: []*datastore.PathElement{
								&datastore.PathElement{
									Kind: db.DomainName,
									Name: key,
								},
							},
						},
						Properties: properties,
					},
				},
			},
		}).Do()
	if err != nil {
		return errors.New("Unable to commit request to GCP datastore: " + err.Error())
	}

	return nil
}

func (db *DatastoreDB) Load(key string, object interface{}) error {
	gql := fmt.Sprintf("SELECT * FROM %s WHERE __key__ = KEY(%s, '%s')",
		db.DomainName, db.DomainName, key)
	fmt.Println(gql)
	resp, err := db.datastoreSvc.Projects.
		RunQuery(testProjectId, &datastore.RunQueryRequest{
			PartitionId: &datastore.PartitionId{
				ProjectId: db.ProjectId,
			},
			GqlQuery: &datastore.GqlQuery{
				AllowLiterals: true,
				QueryString:   gql,
			},
		}).Do()
	if err != nil {
		return errors.New("Unable to select data from GCP datastore: " + err.Error())
	}
	recursiveSetEntityValue(object, resp.Batch.EntityResults[0].Entity.Properties)

	return nil
}

func (db *DatastoreDB) LoadAll(f func() interface{}) (interface{}, error) {
	resp, err := db.datastoreSvc.Projects.
		RunQuery(testProjectId, &datastore.RunQueryRequest{
			PartitionId: &datastore.PartitionId{
				ProjectId: db.ProjectId,
			},
			GqlQuery: &datastore.GqlQuery{
				QueryString: "select * from " + db.DomainName,
			},
		}).Do()
	if err != nil {
		return nil, errors.New("Unable to select data from GCP datastore: " + err.Error())
	}

	items := []interface{}{}
	for _, entityResult := range resp.Batch.EntityResults {
		v := f()
		recursiveSetEntityValue(v, entityResult.Entity.Properties)
		items = append(items, v)
	}

	return items, nil
}

func (db *DatastoreDB) Delete(key string) error {
	_, err := db.datastoreSvc.Projects.
		Commit(db.ProjectId, &datastore.CommitRequest{
			Mode: "NON_TRANSACTIONAL",
			Mutations: []*datastore.Mutation{
				&datastore.Mutation{
					Delete: &datastore.Key{
						PartitionId: &datastore.PartitionId{
							ProjectId: db.ProjectId,
						},
						Path: []*datastore.PathElement{
							&datastore.PathElement{
								Kind: db.DomainName,
								Name: key,
							},
						},
					},
				},
			},
		}).Do()
	if err != nil {
		return errors.New("Unable to delete entity from GCP datastore: " + err.Error())
	}

	return nil
}

func createDatastoreService(serviceAccountPath string) (*datastore.Service, error) {
	dat, err := ioutil.ReadFile(serviceAccountPath)
	if err != nil {
		return nil, errors.New("Unable to read service account file: " + err.Error())
	}

	conf, err := google.JWTConfigFromJSON(dat, datastore.DatastoreScope)
	if err != nil {
		return nil, errors.New("Unable to acquire generate config: " + err.Error())
	}

	client := conf.Client(oauth2.NoContext)
	datastoreSvc, err := datastore.New(client)
	if err != nil {
		return nil, errors.New("Unable to create google cloud platform datastore service: " + err.Error())
	}

	return datastoreSvc, nil
}

func recursiveEntityProperties(props map[string]datastore.Value, v interface{}) error {
	if v == nil || reflect.ValueOf(v).IsNil() {
		return errors.New("Empty interface")
	}

	modelReflect := reflect.ValueOf(v).Elem()
	modelRefType := modelReflect.Type()
	fieldsCount := modelReflect.NumField()

	for i := 0; i < fieldsCount; i++ {
		field := modelReflect.Field(i)
		fieldName := modelRefType.Field(i).Name
		fieldValue := fmt.Sprintf("%v", field.Interface())

		switch field.Kind() {
		case reflect.Interface:
			recursiveEntityProperties(props, field.Interface())
		default:
			_, ok := props[fieldName]
			if ok {
				return errors.New("Unable to set value to properties, duplicate	key name")
			}
			props[fieldName] = datastore.Value{
				StringValue: fieldValue,
			}
		}
	}

	return nil
}

func recursiveSetEntityValue(v interface{}, props map[string]datastore.Value) {
	if v == nil || reflect.ValueOf(v).IsNil() {
		return
	}

	modelReflect := reflect.ValueOf(v).Elem()
	modelRefType := modelReflect.Type()
	fieldsCount := modelReflect.NumField()

	for i := 0; i < fieldsCount; i++ {
		field := modelReflect.Field(i)
		fieldName := modelRefType.Field(i).Name

		switch field.Kind() {
		case reflect.Interface:
			recursiveSetEntityValue(field.Interface(), props)
		default:
			field.Set(reflect.ValueOf(props[fieldName].StringValue))
		}
	}
}

func getProjectId(serviceAccountPath string) (string, error) {
	viper := viper.New()
	viper.SetConfigType("json")
	viper.SetConfigFile(serviceAccountPath)
	err := viper.ReadInConfig()
	if err != nil {
		return "", err
	}
	return viper.GetString("project_id"), nil
}