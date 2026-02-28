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

	exchangeName := "x.logs"

	queues := []struct {
		name string
		key string
	} {
		{name: "q.error", key: "error"},
		{name: "q.info", key: "info"},
	}

	for _, queue := range queues {
		_, err = ch.QueueDeclare(queue.name, true, false, false, false, amqp091.Table{
			"x-queue-type": "quorum",
		})
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}

		err = ch.QueueBind(queue.name, queue.key, exchangeName, false, nil)
		if err != nil {
			log.Fatalln("Failed to bind a queue:", err)
		}
	}

	log.Println("Attempting to publish messages...")

	reader := bufio.NewReader(os.Stdin)
	var selectedQueue string

	for {
		log.Println("Enter queue name to consume from (q.error or q.info):")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read input:", err)
		}
		input = strings.TrimSpace(input)

		if input == queues[0].name || input == queues[1].name {
			selectedQueue = input
			break
		} else {
			log.Println("Invalid queue name. Please enter either:", queues[0].name, "or", queues[1].name)
		}
	}

	fmt.Println("Listening on queue:", selectedQueue)

	msgs, err := ch.Consume(selectedQueue, exchangeName, false, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to consume messages:", err)
	}

	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)

		// SImulate message processing
		// If processing is successful, acknowledge the message
		d.Ack(false)

	}
}