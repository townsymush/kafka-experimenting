package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"test.com/schema"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Fatalf("Delivery failed: %v", ev.TopicPartition)
				} else {
					log.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topic := "enricher"

	for i := 0; i < 10; i++ {
		// generate random number between 1 and 5
		randomNumber := rand.Intn(5) + 1
		time.Sleep(time.Duration(randomNumber) * time.Second)

		t := schema.Transaction{
			ID:        "123" + strconv.Itoa(i),
			Amount:    "100.00",
			AccountID: "456",
			Operation: "withdraw",
			CreatedAt: time.Now(),
		}
		b, err := json.Marshal(t)
		if err != nil {
			log.Fatalf("Failed to marshal transaction: %s", err)
		}
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          b,
		}, nil)
		fmt.Println("Sent message")
	}

	p.Flush(15 * 1000)
}
