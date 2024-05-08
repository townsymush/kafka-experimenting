package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

// Environment Constants
const (
	// KafkaBroker is the address of the Kafka broker
	kafkaBroker = "KAFKA_BROKER"
	// KafkaTopic is the topic to which the messages are produced
	StorerTopic = "STORER_KAFKA_TOPIC"

	// PollingInterval is the interval at which the consumer polls for messages in seconds
	PollingInterval = "POLLING_INTERVAL"

	// Postgres credential environment variables
	PostgresUser     = "POSTGRES_USER"
	PostgresPassword = "POSTGRES_PASSWORD"
	PostgresHost     = "POSTGRES_HOST"
	PostgresDB       = "POSTGRES_DB"
	PostgresPort     = "POSTGRES_PORT"

	// ConsumerWorkers is the number of workers to process messages from the Kafka Topic
	ConsumerWorkers = "CONSUMER_WORKERS"
	ConsumerGroup   = "CONSUMER_GROUP"
)

// Config holds the application configuration, mostly from environment variables
type Config struct {
	Broker          string
	StorerTopic     string
	PollingInterval int
	DBURL           string
	Workers         int
	ConsumerGroup   string
}

func ParseConfig() (Config, error) {
	broker := os.Getenv(kafkaBroker)
	if broker == "" {
		return Config{}, fmt.Errorf("%s is not set", kafkaBroker)
	}

	workers := os.Getenv(ConsumerWorkers)
	if workers == "" {
		return Config{}, fmt.Errorf("%s is not set", ConsumerWorkers)
	}

	numWorkers, err := strconv.Atoi(workers)
	if err != nil {
		return Config{}, fmt.Errorf("%s is not a valid integer", workers)
	}

	pollingInterval := os.Getenv(PollingInterval)
	p, err := strconv.Atoi(pollingInterval)
	if err != nil {
		return Config{}, fmt.Errorf("%s is not a valid integer", pollingInterval)
	}

	if p == 0 {
		return Config{}, fmt.Errorf("%s is not set", PollingInterval)
	}

	err = validateDBDredentials()
	if err != nil {
		return Config{}, err
	}

	ConsumerGroup := os.Getenv(ConsumerGroup)
	if ConsumerGroup == "" {
		return Config{}, fmt.Errorf("%s is not set", ConsumerGroup)
	}

	return Config{
		Broker:          broker,
		StorerTopic:     os.Getenv(StorerTopic),
		PollingInterval: p,
		ConsumerGroup:   ConsumerGroup,
		DBURL:           createDBURL(),
		Workers:         numWorkers,
	}, nil
}

func createDBURL() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		os.Getenv(PostgresUser),
		os.Getenv(PostgresPassword),
		os.Getenv(PostgresHost),
		os.Getenv(PostgresPort),
		os.Getenv(PostgresDB),
	)
}

func validateDBDredentials() error {
	if os.Getenv("POSTGRES_USER") == "" {
		return errors.New("POSTGRES_USER is not set")
	}

	if os.Getenv("POSTGRES_PASSWORD") == "" {
		return errors.New("POSTGRES_PASSWORD is not set")
	}

	if os.Getenv("POSTGRES_HOST") == "" {
		return errors.New("POSTGRES_HOST is not set")
	}

	if os.Getenv("POSTGRES_DB") == "" {
		return errors.New("POSTGRES_DB is not set")
	}

	if os.Getenv("POSTGRES_PORT") == "" {
		return errors.New("POSTGRES_PORT is not set")
	}
	return nil
}
