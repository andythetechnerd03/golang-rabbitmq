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

	queues := []string {"q.admin", "q.user"}

	for _, queue := range queues {
		_, err = ch.QueueDeclare(queue, true, false, false, false, amqp091.Table{
			"x-queue-type": "quorum",
		})
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}
	}

	log.Println("Attempting to consume messages...")

	reader := bufio.NewReader(os.Stdin)
	var selectedQueue string

	for {
		log.Printf("Enter queue name to consume from (%v/%v): ", queues[0], queues[1])
		userType, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read queue name to consume from ", err)
		}
		userType = strings.TrimSpace(strings.ToLower(userType))

		if userType == "quit" {
			log.Println("Exiting producer...")
			continue
		}
		
		if userType == queues[0] || userType == queues[1] {
			selectedQueue = userType
			break
		}
		log.Printf("Invalid queue name. Please enter either %v or %v.", queues[0], queues[1])
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