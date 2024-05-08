package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"test.com/storer-consumer/storage"
)

// Worker is the struct that represents the worker that processes messages from the incoming channel
type Worker struct {
	ID              int
	consumer        *kafka.Consumer
	incomingChannel <-chan *kafka.Message
	db              storage.Transactioner
}

// New creates a new worker, it requires the channel to listen for incoming messages and the storage layer to write to
func New(id int, consumer *kafka.Consumer, incomingChannel <-chan *kafka.Message, db storage.Transactioner) *Worker {
	return &Worker{
		ID:              id,
		consumer:        consumer,
		incomingChannel: incomingChannel,
		db:              db,
	}
}

// Work is the main function that processes messages from the incoming channel
// it writes the transaction to the database and commits the message
func (w *Worker) Work(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-w.incomingChannel:
			var transaction storage.Transaction
			err := json.Unmarshal(msg.Value, &transaction)
			if err != nil {
				// todo send to DLQ
				fmt.Printf("Failed to unmarshal message: %s \n", err)

				// we would commit the message once it had been sent to the dlq
				w.consumer.CommitMessage(msg)
				continue

			}

			// if err = w.db.SaveTransaction(ctx, transaction); err != nil {
			// 	// todo retry based on error / send to DLQ
			// 	fmt.Printf("Failed to save transaction: %s \n", err)

			// 	// we would commit the message once it had been sent to the dlq
			// 	w.consumer.CommitMessage(msg)
			// 	continue
			// }

			fmt.Printf("Worker %d saved message on %s to database\n", w.ID, string(msg.Value))
			w.consumer.CommitMessage(msg)
		}
	}
}
