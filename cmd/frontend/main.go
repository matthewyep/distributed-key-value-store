package main

import (
	"fmt"
	"log"

	distkvs "example.org/cpsc416/a5"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config distkvs.FrontEndConfig
	err := distkvs.ReadJSONConfig("config/frontend_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(config)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "frontend",
		Secret:         config.TracerSecret,
	})

	frontend := distkvs.FrontEnd{}
	err = frontend.Start(config.ClientAPIListenAddr, config.StorageAPIListenAddr, 0, tracer)

	if err != nil {
		log.Fatal(err)
	}
}
