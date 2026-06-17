package main

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rafaelcmr/msgbroker-go/common"
)

var (
	amqpUser = "guest"
	amqpPass = "guest"
	amqpHost = "localhost"
	amqpPort = "5672"
)

func main() {
	ch, closeConn := common.ConnectAmqp(amqpUser, amqpPass, amqpHost, amqpPort)
	defer func() {
		closeConn()
		ch.Close()
	}()

	listen(ch)
}

func listen(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(common.OrderCreatedEvent, true, false, false, false,
		nil)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

			o := &common.Order{}
			if err := json.Unmarshal(d.Body, o); err != nil {
				log.Printf("failed to unmarshal order: %v", err)
				d.Nack(false, false)
				continue
			}

			paymentLink, err := createPaymentLink()
			if err != nil {
				log.Printf("failed to create payment: %v", err)
				d.Nack(false, true)

				//handle retry here
				continue
			}

			log.Printf("Payment link generated: %s", paymentLink)
			d.Ack(false)
		}
	}()

	log.Printf("AMQP Listening. To exit press CTRL+C")
	<-forever
}

func createPaymentLink() (string, error) {
	return "created-payment-link.com", nil
}
