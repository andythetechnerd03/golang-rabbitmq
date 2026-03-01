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

	exchangeName := "x.auto.delete"

	err = ch.ExchangeDeclare(exchangeName, "fanout", true, true, false, false, nil) // auto-delete exchange
	if err != nil {
		log.Fatalln("Failed to declare an exchange:", err)
	}

	q, err := ch.QueueDeclare("", true, true, false, false, nil) // auto-delete, exclusive, non-durable queue
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	err = ch.QueueBind(q.Name, "", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind a queue:", err)
	}

	msgs, err := ch.Consume(q.Name, "andy", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to register a consumer:", err)
	}

	log.Println("Waiting for messages. To exit press CTRL+C")
	for d := range msgs {
		log.Printf("Received message: %s", d.Body)
	}
}