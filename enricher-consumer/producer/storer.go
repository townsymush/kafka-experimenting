package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"test.com/consumer/config"
)

// Producer is the struct that represents the kafka producer as a reusable component
type Producer struct {
	k     *kafka.Producer
	topic string
}

// New creates a new kafka producer wrapper
func New(cfg config.Config, topic string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Broker,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		k:     p,
		topic: topic,
	}, nil
}

// Dispatch sends a message to the kafka topic
func (p *Producer) Dispatch(in []byte) error {
	return p.k.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          in,
	}, nil)
}
