package enrichment

import (
	"encoding/json"
	"fmt"
	"time"

	"test.com/schema"
)

// producer is the interface that defines the method to dispatch a message
type producer interface {
	Dispatch(in []byte) error
}

// Processor is the struct that represents the enrichment processor
type Enricher struct {
	producer producer
}

func New(producer producer) *Enricher {
	return &Enricher{
		producer: producer,
	}
}

// Process takes the incoming transaction data, enriches it and sends it to the next topic
func (e *Enricher) Process(in []byte) error {
	// enrich the transaction
	enrichedTransaction, err := enrichTransaction(in)
	if err != nil {
		return fmt.Errorf("failed to enrich transaction: %w", err)
	}

	// send the enriched transaction to the next topic
	err = e.producer.Dispatch(enrichedTransaction)
	if err != nil {
		return fmt.Errorf("failed to dispatch enriched transaction: %w", err)
	}

	return nil
}

// enrichTransaction enriches the transaction data
func enrichTransaction(in []byte) ([]byte, error) {
	// unmarshal the transaction
	var transaction schema.Transaction
	err := json.Unmarshal(in, &transaction)
	if err != nil {
		return []byte{}, err
	}

	// todo would make request for enriched data here e.g. MongoDB

	// enrich the transaction
	enrichedTransaction := schema.EnrichedTransaction{
		Transaction: transaction,
		Name:        "John Doe",
		AccountType: "Savings",
		ProcessedAt: time.Now(),
	}

	out, err := json.Marshal(enrichedTransaction)
	if err != nil {
		return nil, err
	}

	return out, nil
}
