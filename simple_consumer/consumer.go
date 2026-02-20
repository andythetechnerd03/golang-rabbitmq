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
		false, // durable
		false, // autodelete
		false, // exclusive
		false, // nowait
		nil, // args
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue name
		"", // consumer tag
		true, // auto ack
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
	}
}
