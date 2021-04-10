package main

import (
	"flag"
	"log"

	distkvs "example.org/cpsc416/a5"
	"example.org/cpsc416/a5/kvslib"
)

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err, _ := client.Get("clientID1", "key1"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put("clientID1", "key2", "value2"); err != 0 {
		log.Println(err)
	}

	for i := 0; i < 2; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}
}
