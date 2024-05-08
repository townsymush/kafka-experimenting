package schema

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

func TestValidateTransactionSchema(t *testing.T) {

	sch := NewSchemaValidatior(t, "transaction.json")

	transaction := Transaction{
		ID:        "123",
		Amount:    "100.00",
		AccountID: "456",
		Operation: "withdraw",
		CreatedAt: time.Now(),
	}
	transactionBytes, err := json.Marshal(transaction)
	if err != nil {
		t.Fatalf("failed to marshal transaction: %v", err)
		t.FailNow()
	}

	fmt.Println(string(transactionBytes))

	var v any
	json.Unmarshal(transactionBytes, &v)
	if err := sch.Validate(v); err != nil {
		t.Fatalf("transaction schema validation failed: %v", err)
		t.FailNow()
	}
}

func TestValidateTransactionEnrichedSchema(t *testing.T) {
	sch := NewSchemaValidatior(t, "transaction_enriched.json")

	enrichedTransaction := EnrichedTransaction{
		Transaction: Transaction{
			ID:        "123",
			Amount:    "100.00",
			AccountID: "456",
			Operation: "withdraw",
			CreatedAt: time.Now(),
		},
		Name:        "John Doe",
		AccountType: "savings",
	}

	transactionBytes, err := json.Marshal(enrichedTransaction)
	if err != nil {
		t.Fatalf("failed to marshal transaction: %v", err)
		t.FailNow()
	}

	fmt.Println(string(transactionBytes))

	var v any
	json.Unmarshal(transactionBytes, &v)
	if err := sch.Validate(v); err != nil {
		t.Fatalf("transaction schema validation failed: %v", err)
		t.FailNow()
	}
}

func NewSchemaValidatior(t *testing.T, path string) *jsonschema.Schema {
	sch, err := jsonschema.Compile("transaction.json")
	if err != nil {
		t.Fatalf("failed to read schema file: %v", err)
		t.FailNow()
	}
	return sch
}
