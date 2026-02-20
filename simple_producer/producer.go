package main

import (
	"log"
	"github.com/rabbitmq/amqp091-go"
	"time"
)

func main() {
	// connect to RMQ
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

	// Declare a queue
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

	// Publish a message
	err = ch.Publish(
		"", // nameless exchange
		q.Name, // routing key (queue name)
		false, // mandatory
		false, // immediate
		amqp091.Publishing{
			ContentType: "text/plain",
			Body: []byte("Hello, RabbitMQ!"),
		},
	)
	if err != nil {
		log.Fatalln("Failed to publish a message:", err)
	}
	time.Sleep(15 * time.Second) // wait for the consumer to process the message before exiting
	log.Println("Message published successfully")
}
