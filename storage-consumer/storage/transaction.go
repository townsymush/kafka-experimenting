package storage

// Transaction is the struct that represents the transaction entity from the storage layer
type Transaction struct {
	// ID is the unique identifier of the transaction
	ID string
	// Amount is the amount of the transaction
	// storing as a string due to no mathematically operations required
	Amount string
	// Status is the status of the transaction
	Status string
	// CreatedAt is the time when the transaction was created
	CreatedAt string
}
