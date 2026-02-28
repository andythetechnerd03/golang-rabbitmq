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

	queues := []struct {
		name string
		pattern string
	} {
		{name: "q.error", pattern: "error.#"}, // # wildcard matches 0 or more words
		{name: "q.info", pattern: "*.database"}, // 1 word wildcard
	}
	exchangeName := "x.topic.logs"

	err = ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare an exchange:", err)
	}

	for _, queue := range queues {
		q, err := ch.QueueDeclare(queue.name, true, false, false, false, amqp091.Table{
			"x-queue-type": "quorum",
		})
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}

		err = ch.QueueBind(q.Name, queue.pattern, exchangeName, false, nil)
		if err != nil {
			log.Fatalln("Failed to bind a queue:", err)
		}
	}

	log.Println("Attempting to consume messages...")

	reader := bufio.NewReader(os.Stdin)
	var selectedQueue string

	for {
		log.Printf("Enter queue name to consume from (%v/%v): ", queues[0].name, queues[1].name)
		userType, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read queue name to consume from ", err)
		}
		userType = strings.TrimSpace(strings.ToLower(userType))
		
		if userType == queues[0].name || userType == queues[1].name {
			selectedQueue = userType
			break
		}
		log.Printf("Invalid queue name. Please enter either %v or %v.", queues[0].name, queues[1].name)
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