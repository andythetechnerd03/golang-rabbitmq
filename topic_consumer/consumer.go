package main

import (
	"bufio"
	"log"
	"os"
	"strings"
	"fmt"
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

	queueNames := []string{"queue1", "queue2"}

	for _, name := range queueNames {
		_, err := ch.QueueDeclare(name, true, false, false, false, amqp091.Table{
			"x-queue-type": "quorum",
		})
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}
	}

	reader := bufio.NewReader(os.Stdin)
	var selectedQueue string

	for {
		log.Println("Enter queue name to consume from (queue1 or queue2):")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read input:", err)
		}
		input = strings.TrimSpace(input)

		if input == queueNames[0] || input == queueNames[1] {
			selectedQueue = input
			break
		} else {
			log.Println("Invalid queue name. Please enter either:", queueNames[0], "or", queueNames[1])
		}
	}

	fmt.Println("Listening on queue:", selectedQueue)

	msgs, err := ch.Consume(selectedQueue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to consume messages:", err)
	}

	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)

		// SImulate message processing
		// If processing is successful, acknowledge the message
		d.Ack(false)

		// If processing fails and you want to requeue the message, you can use:
		// d.Nack(false, true) // requeue = True

		// If processing fails and you want to discard the message (to DLX), you can use:
		// d.Reject(false)
	}
}