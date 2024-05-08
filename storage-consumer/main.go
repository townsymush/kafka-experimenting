package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"test.com/storer-consumer/config"
	"test.com/storer-consumer/storage"
	"test.com/storer-consumer/worker"
)

func main() {
	incomingChannel := make(chan *kafka.Message)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.ParseConfig()
	if err != nil {
		log.Fatalf("Failed to parse config: %s", err)
	}

	// listen for termination signals to exit gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// listen for termination signals to exit gracefully
	go func() {
		<-sigChan
		fmt.Println("Received termination signal, exiting gracefully...")
		cancel()
	}()

	// Bootstrap consumer to read messages from the storer topic
	consumer, err := createConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	// Bootstrap connection to the database
	pool, err := storage.PgConnect(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %s", err)
	}
	defer pool.Close()

	// Create a new transactional database layer
	db := storage.NewTransactionalDatabase(pool)

	// Create workers based on config to listen to messages from the consumer
	for i := 0; i < cfg.Workers; i++ {
		w := worker.New(i, consumer, incomingChannel, db)
		go w.Work(ctx)
	}

	go startConsuming(ctx, cfg, consumer, incomingChannel)

	<-ctx.Done()
}

func createConsumer(cfg config.Config) (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Broker,
		"group.id":           cfg.ConsumerGroup,
		"auto.offset.reset":  "earliest",        // todo
		"enable.auto.commit": "false",           // "true" by default
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// subscribe to the storer topic
	if err = c.SubscribeTopics([]string{cfg.StorerTopic}, nil); err != nil {
		log.Fatalf("Error subscribing to topics: %s", err)
	}

	return c, nil
}

func startConsuming(ctx context.Context, cfg config.Config, c *kafka.Consumer, incomingChannel chan<- *kafka.Message) {
	defer close(incomingChannel)
	for {
		select {
		case <-ctx.Done():
			c.Close()
			return
		default:
			msg, err := c.ReadMessage(time.Duration(cfg.PollingInterval) * time.Second)
			if err != nil {
				if err.(kafka.Error).IsTimeout() {
					continue
				}
				fmt.Printf("Error reading message on topic: %v, error: %s \n", msg, err)
				c.Close()
				return
			}

			// send kafka message to channel
			incomingChannel <- msg
		}
	}
}
