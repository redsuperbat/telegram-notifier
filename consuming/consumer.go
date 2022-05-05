package consuming

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConsumerFunc = func(kafka.Message) error

func NewConsumer(c chan []byte, groupId string) (func(), func() error) {
	// make a new reader that consumes from topic-A
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		log.Fatalln("A topic must be provided")
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		GroupID:     groupId,
		Topic:       kafkaTopic,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		Partition:   0,
		StartOffset: kafka.LastOffset,
		MaxWait:     time.Millisecond * 200,
	})

	return func() {
			log.Println("Starting consumer...")
			for {
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					log.Println(err)
					continue
				}
				c <- m.Value
			}
		}, func() error {
			log.Println("Closing connection to kafka...")
			return r.Close()
		}
}
