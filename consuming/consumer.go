package consuming

import (
	"context"
	"log"
	"os"

	"github.com/EventStore/EventStore-Client-Go/esdb"
)

func NewEsClient() *esdb.Client {
	uri := os.Getenv("EVENTSTORE_URI")
	settings, err := esdb.ParseConnectionString(uri)

	if err != nil {
		log.Fatalln(err)
	}

	db, err := esdb.NewClient(settings)
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

func NewConsumer(c chan []byte, groupId string) (func(), func() error) {

	client := NewEsClient()

	err := client.CreatePersistentSubscription(context.Background(), "chat-stream", "telegram-notifier", esdb.PersistentStreamSubscriptionOptions{})

	serr, ok := err.(*esdb.PersistentSubscriptionError)
	if !ok {
		log.Fatalln(err)
	}

	if serr.Code != 6 {
		log.Fatalln(serr)
	}

	sub, err := client.ConnectToPersistentSubscription(context.Background(), "chat-stream", "telegram-notifier", esdb.ConnectToPersistentSubscriptionOptions{})
	if err != nil {
		log.Fatalln(err)
	}

	return func() {
			log.Println("Starting consumer...")
			for {
				e := sub.Recv()
				if e.SubscriptionDropped != nil {
					log.Println(e.SubscriptionDropped.Error)
					break
				}
				if e.EventAppeared == nil {
					continue
				}
				log.Println("Received event", string(e.EventAppeared.Event.Data))
				if e.EventAppeared.Event.EventType != "ChatCreatedEvent" {
					sub.Ack(e.EventAppeared)
					continue
				}
				c <- e.EventAppeared.Event.Data
				sub.Ack(e.EventAppeared)
			}
		}, func() error {
			log.Println("Closing connection to kafka...")
			return sub.Close()
		}
}
