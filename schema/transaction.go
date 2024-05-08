package schema

import "time"

// Transaction is from the incoming data that is published to the Kafka topic
type Transaction struct {
	ID string `json:"id"`
	// Amount is a string to avoid floating point precision issues
	Amount    string    `json:"amount"`
	AccountID string    `json:"account_id"`
	Operation string    `json:"operation"`
	CreatedAt time.Time `json:"created_at"`
}

// EnrichedTransaction is the enriched data that is published to the Kafka topic
type EnrichedTransaction struct {
	Transaction
	Name        string    `json:"name"`
	AccountType string    `json:"account_type"`
	ProcessedAt time.Time `json:"processed_at"`
}
