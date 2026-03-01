package main

import (
	"log"
	"github.com/rabbitmq/amqp091-go"
)

func main () {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to open a channel:", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // queue name
		true, // durable for quorum
		false, // autodelete
		false, // exclusive
		false, // nowait
		amqp091.Table{
			"x-queue-type": "quorum",
		}, // args
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue name
		"", // consumer tag
		false, // auto ack, false will keep program running, true will stop after consuming all
		false, // exclusive
		false, // no local
		false, // no wait
		nil, // args
	)
	if err != nil {
		log.Fatalln("Failed to register a consumer:", err)
	}

	for msg := range msgs {
		log.Println("Received a message:", string(msg.Body))

		// If processing successful, acknowledge the message
		// msg.Ack(false) // false means 'ack this message only'. true in case of batch processing

		// msg.Nack(false, true)
		// If auto-ack false and requeue true, there will be a 21-message loop of quorum queue redelivery.
		msg.Reject(false)
	}
}
