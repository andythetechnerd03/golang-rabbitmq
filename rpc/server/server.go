package main

import (
	"log"
	"strings"
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

	// Declare an RPC queue for receiving requests
	q, err := ch.QueueDeclare("q.rpc", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}
	log.Printf("Declared RPC queue: %s", q.Name)

	// Consume messages from the RPC queue
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to register a consumer:", err)
	}

	log.Println("Waiting for RPC requests. To exit press CTRL+C")
	for msg := range msgs {
		processedText := strings.ToUpper(string(msg.Body))
		
		log.Printf("Received: %s | responding with: %s", msg.Body, processedText)

		// Publish response to the reply-to queue specified in the request message, using the correlation ID for matching
		err = ch.Publish("", msg.ReplyTo, false, false, amqp091.Publishing{
			ContentType:   "text/plain",
			CorrelationId: msg.CorrelationId,
			Body:          []byte(processedText),
		})
		if err != nil {
			log.Println("Failed to publish response:", err)
		}

		msg.Ack(false) // Acknowledge the message
	}

}