package main

import (
	"log"
	"github.com/rabbitmq/amqp091-go"
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

	// Declare the same queue as the producer
	queueName := "q.transaction"
	_, err = ch.QueueDeclare(
		queueName, // Queue name
		false,               // Durable
		false,               // Auto-delete
		false,               // Exclusive
		false,               // No-wait
		nil,                 // Arguments
	)
	if err != nil {
		log.Fatal("Failed to declare a queue:", err)
	}

	// Consume messages from the queue
	msgs, err := ch.Consume(
		queueName, // Queue name
		"",        // Consumer tag
		true,      // Auto-ack
		false,     // Exclusive
		false,     // No-local
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		log.Fatal("Failed to register a consumer:", err)
	}

	log.Println("Waiting for transactional messages. To exit press CTRL+C")
	for d := range msgs {
		log.Printf("Received message: %s", d.Body)
	}
}