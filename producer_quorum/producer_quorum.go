package main

import (
	"log"
	"github.com/rabbitmq/amqp091-go"
	"fmt"
)

func main() {
	// connect to RMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/qu")

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
		true, // durable has to be true for quorum queues
		false, // autodelete
		false, // exclusive
		false, // nowait
		amqp091.Table{
			"x-queue-type": "quorum",
		}, // args, has to include x-queue-type: quorum for quorum queues
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}
	log.Println("Connected and quorum queue declared")

	// log.Println("Stopping RabbitMQ Container...")
	// cmd := exec.Command("docker", "stop", "rabbitmq")
	// if err := cmd.Run(); err != nil {
	// 	log.Fatalln("Failed to stop RabbitMQ container:", err)
	// }
	// time.Sleep(time.Second)

	// Declare context
	msg := "Hello, RabbitMQ!"
	if msg == "fuck" {fmt.Println("No bad words allowed!")}
	log.Printf("Publishing message: %s\n", msg)

	// Publish a message
	err = ch.Publish(
		"", // nameless exchange
		q.Name, // routing key (queue name)
		false, // mandatory
		false, // immediate
		amqp091.Publishing{
			ContentType: "text/plain",
			Body: []byte(msg),
		},
	)
	if err != nil {
		log.Fatalln("Failed to publish a message:", err)
	}
	log.Println("Sent:", msg)

	// log.Println("Restarting RabbitMQ Container...")
	// cmd = exec.Command("docker", "start", "rabbitmq")
	// if err := cmd.Run(); err != nil {
	// 	log.Fatalln("Failed to start RabbitMQ container:", err)
	// }
	// log.Println("RabbitMQ container restarted")
}
