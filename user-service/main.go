package main

import (
	"context"
	"log"

	"github.com/O7Oghany/EDM/user-service/common"
	"github.com/O7Oghany/EDM/user-service/internal"
)

func main() {

	//var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var consumer internal.Consumer
	var producer internal.Producer

	producerCfg, consumerCfg, err := common.LoadConfigs()
	if err != nil {
		log.Fatalf("Error loading configs: %v", err)
	}

	//wg.Add(1)
	go func(ctx context.Context) {
		//defer wg.Done()
		log.Printf("will start the go routine for consuming events")
		consumer.ConsumeEvents(consumerCfg, ctx, cancel)
		log.Printf("after")
	}(ctx)

	if err := producer.StartProducerServer(producerCfg); err != nil {
		log.Fatalf("Error starting producer server: %v", err)
	}
	//wg.Wait()
}
