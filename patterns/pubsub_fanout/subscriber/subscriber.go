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
	exchangeName := "x.logs"

	for _, name := range queueNames {
		q, err := ch.QueueDeclare(name, true, false, false, false, amqp091.Table{
			"x-queue-type": "quorum",
		})
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}
		
		err = ch.QueueBind(q.Name, "", exchangeName, false, nil)
		if err != nil {
			log.Fatalln("Failed to bind a queue:", err)
		}
		log.Printf("Declared and bound queue: %s", q.Name)
	}

	log.Println("Attempting to publish messages...")

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

	}
}