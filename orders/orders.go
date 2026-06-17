package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rafaelcmr/msgbroker-go/common"
)

var (
	amqpUser = "guest"
	amqpPass = "guest"
	amqpHost = "localhost"
	amqpPort = "5672"
)

func main() {
	ch, close := common.ConnectAmqp(amqpUser, amqpPass, amqpHost, amqpPort)
	defer func() {
		close()
		ch.Close()
	}()

	q, err := ch.QueueDeclare(common.OrderCreatedEvent, true, false, false, false,
		nil)
	if err != nil {
		log.Fatal(err)
	}

	marshalledOrder, err := json.Marshal(common.Order{
		ID: "order-1",
		Items: []common.Item{
			{
				ID:       "item-1",
				Quantity: 1,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	err = ch.PublishWithContext(context.Background(), "", q.Name, false, false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        marshalledOrder,
		})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Order published")
}
