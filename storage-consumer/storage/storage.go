package storage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionalDatabase struct {
	// pool is the connection pool to the database
	pool *pgxpool.Pool
}

func NewTransactionalDatabase(pool *pgxpool.Pool) *TransactionalDatabase {
	return &TransactionalDatabase{
		pool: pool,
	}
}

// SaveTransaction saves the transaction in the storage
func (t *TransactionalDatabase) SaveTransaction(ctx context.Context, transaction Transaction) error {
	_, err := t.pool.Exec(context.Background(), `
		INSERT INTO transactions (amount, status, created_at)
		VALUES ($1, $2, $3, $4)
	`, transaction.ID, transaction.Amount, transaction.Status, transaction.CreatedAt)
	if err != nil {
		return err
	}
	return nil
}
