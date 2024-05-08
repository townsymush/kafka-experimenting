package config

import (
	"fmt"
	"os"
	"strconv"
)

// KAFKA_BROKER: kafka:9092
// KAFKA_TOPIC: test

const (
	// KafkaBroker is the address of the Kafka broker
	kafkaBroker = "KAFKA_BROKER"
	// KafkaTopic is the topic to which the messages are produced
	StorerTopic   = "STORER_KAFKA_TOPIC"
	EnricherTopic = "ENRICHER_KAFKA_TOPIC"

	// PollingInterval is the interval at which the consumer polls for messages in seconds
	PollingInterval = "POLLING_INTERVAL"

	// Postgres credential environment variables
	PostgresUser     = "POSTGRES_USER"
	PostgresPassword = "POSTGRES_PASSWORD"
	PostgresHost     = "POSTGRES_HOST"
	PostgresDB       = "POSTGRES_DB"
	PostgresPort     = "POSTGRES_PORT"
)

// Config holds the application configuration, mostly from environment variables
type Config struct {
	Broker          string
	EnricherTopic   string
	StorerTopic     string
	PollingInterval int
}

func ParseConfig() (Config, error) {
	broker := os.Getenv(kafkaBroker)
	if broker == "" {
		return Config{}, fmt.Errorf("%s is not set", kafkaBroker)
	}

	enricherTopic := os.Getenv(EnricherTopic)
	if enricherTopic == "" {
		return Config{}, fmt.Errorf("%s is not set", enricherTopic)
	}

	pollingInterval := os.Getenv(PollingInterval)
	p, err := strconv.Atoi(pollingInterval)
	if err != nil {
		return Config{}, fmt.Errorf("%s is not a valid integer", pollingInterval)
	}

	if p == 0 {
		return Config{}, fmt.Errorf("%s is not set", PollingInterval)
	}

	return Config{
		Broker:          broker,
		EnricherTopic:   enricherTopic,
		StorerTopic:     os.Getenv(StorerTopic),
		PollingInterval: p,
	}, nil
}
