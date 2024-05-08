package storage

import "context"

type Transactioner interface {
	// SaveTransaction saves the transaction in the storage
	SaveTransaction(ctx context.Context, transaction Transaction) error
}
