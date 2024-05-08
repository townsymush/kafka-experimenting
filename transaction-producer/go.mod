module test.com/kafka-producer

go 1.22.1

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	test.com/schema v0.0.0-00010101000000-000000000000
)

replace test.com/schema => ../schema
