package blobstore

func getDomainName(name string, config BlobStoreConfig) string {
	return name + config.GetString("store.domainPostfix")
}
