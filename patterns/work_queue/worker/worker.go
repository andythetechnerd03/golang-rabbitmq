package main

import (
	"log"
	"time"
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

	q, err := ch.QueueDeclare("task_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}
	log.Printf("Declared queue: %s", q.Name)

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to register a consumer:", err)
	}

	log.Println("Waiting for messages. To exit press CTRL+C")
	for d := range msgs {
		log.Printf("Processing: %s", d.Body)
		time.Sleep(time.Duration(len(d.Body)) * time.Millisecond * 300) // Simulate work
		log.Println("Done processing message:", string(d.Body))

		d.Ack(false) // Acknowledge the message
	}


}