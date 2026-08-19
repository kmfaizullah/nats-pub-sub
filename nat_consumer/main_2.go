package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type PaymentCreatedEvent struct {
	PaymentID string `json:"payment_id"`
	Amount    int    `json:"amount"`
	Currency  string `json:"currency"`
}

func main() {
	ctx := context.Background()

	nc, err := nats.Connect("nats://localhost:14222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Connected to NATS")

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	stream, err := js.Stream(ctx, "PAYMENT_EVENTS")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Found stream:", stream.CachedInfo().Config.Name)

	// consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
	// 	Name:          "payment-worker",
	// 	// DurableName:   "payment-worker",
	// 	AckPolicy:     jetstream.AckExplicitPolicy,
	// 	AckWait:        30 * time.Second,
	// 	MaxDeliver:     5,
	// 	FilterSubject: "payment.created",
	// 	DeliverPolicy:  jetstream.DeliverAllPolicy,
	// })

	consumer, err := stream.Consumer(ctx, "payment-worker")
	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Consumer ready: payment-worker")

	iter, err := consumer.Messages()
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Stop()

	go func() {
		for {
			msg, err := iter.Next()
			if err != nil {
				log.Printf("consumer error: %v", err)
				continue
			}

			var event PaymentCreatedEvent

			if err := json.Unmarshal(msg.Data(), &event); err != nil {
				log.Printf("failed to decode message: %v", err)
				continue
			}

			log.Printf(
				"Payment created: id=%s amount=%d currency=%s",
				event.PaymentID,
				event.Amount,
				event.Currency,
			)

			// if err := msg.Ack(); err != nil {
			// 	log.Printf("failed to ACK message: %v", err)
			// 	continue
			// }

			log.Printf("ACK sent for payment: %s", event.PaymentID)
		}
	}()

	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "ok",
		})
	})

	log.Fatal(app.Listen(":8081"))
}