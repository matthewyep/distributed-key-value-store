package main

import (
	"flag"
	"github.com/DistributedClocks/tracing"
	"log"

	distkvs "example.org/cpsc416/a5"
)

func main() {
	var config distkvs.StorageConfig
	err := distkvs.ReadJSONConfig("config/storage_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.StorageID, "id", config.StorageID, "Storage ID, e.g. storage1")

	var storageAdd string
	flag.StringVar(&storageAdd, "listen", string(config.StorageAdd), "IP addr to listen on")
	flag.Parse()

	config.StorageAdd = distkvs.StorageAddr(storageAdd)
	log.Println(config)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.StorageID,
		Secret:         config.TracerSecret,
	})

	storage := distkvs.Storage{}
	err = storage.Start(config.StorageID, config.FrontEndAddr, string(config.StorageAdd), config.DiskPath, tracer)
	if err != nil {
		log.Fatal(err)
	}
}
