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

	msg := "Hello, RabbitMQ! This message will be sent to an auto-delete exchange."
	err = ch.Publish(exchangeName, "", false, false, amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg),
	})
	if err != nil {
		log.Fatalln("Failed to publish a message:", err)
	}

	log.Println("Message published to auto-delete exchange. The exchange will be deleted when there are no more queues bound to it.")
}