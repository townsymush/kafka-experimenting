package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"test.com/consumer/config"
	"test.com/consumer/enrichment"
	"test.com/consumer/producer"
)

func main() {
	// ctx := context.Background()
	cfg, err := config.ParseConfig()
	if err != nil {
		log.Fatalf("Failed to parse config: %s", err)
	}

	// // establish connection to the database
	// pool, err := storage.Connect(ctx, cfg)
	// if err != nil {
	// 	log.Fatalf("Failed to connect to the database: %s", err)
	// }
	// defer pool.Close()

	// // create a new transactional database
	// tdb := storage.NewTransactionalDatabase(pool)

	// start never ending consumer polling every 2 seconds

	// create producer for storing enchanced transaction data
	p, err := producer.New(cfg, cfg.StorerTopic)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	// create the enrichment service
	e := enrichment.New(p)

	listen(cfg, e)
}

func listen(cfg config.Config, e *enrichment.Enricher) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Broker,
		"group.id":          "myGroup",  // todo
		"auto.offset.reset": "earliest", // todo
		// look at disabling auto commit for dr
		// needs idempotency check
		// needs DLQ
	})

	defer func() {
		if err := c.Close(); err != nil {
			fmt.Printf("Failed to close consumer: %v", err)
		}
	}()

	if err != nil {
		log.Fatal(err)
	}

	// subscribe to the enricher topic
	if err = c.SubscribeTopics([]string{cfg.EnricherTopic}, nil); err != nil {
		log.Fatalf("Error subscribing to topics: %s", err)
	}

	for {
		msg, err := c.ReadMessage(time.Duration(cfg.PollingInterval) * time.Second)
		if err != nil {
			if err.(kafka.Error).IsTimeout() {
				continue
			}
			fmt.Printf("Error reading message on topic: %v, error: %s \n", msg, err)
			c.Close()
			return
		}

		if err = e.Process(msg.Value); err != nil {
			fmt.Printf("Failed to process message: %s\n", err)
			// todo send to DLQ after retries
			continue
		}
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	}
}
