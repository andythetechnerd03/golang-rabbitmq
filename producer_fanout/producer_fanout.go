package main

import (
	"bufio"
	"log"
	"os"
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

	err = ch.ExchangeDeclare(
		"x.logs", // exchange name
		"fanout", // exchange type
		true, // durable
		false, // autodelete
		false, // internal
		false, // nowait
		nil, // args
	)
	if err != nil {
		log.Fatalln("Failed to declare an exchange:", err)
	}

	// Declare a queue
	q1, err := ch.QueueDeclare(
		"queue1", // queue name
		true, // durable
		false, // autodelete
		false, // exclusive
		false, // nowait
		amqp091.Table{
			"x-queue-type": "quorum",
		}, // args
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	// Declare a queue
	q2, err := ch.QueueDeclare(
		"queue2", // queue name
		true, // durable
		false, // autodelete
		false, // exclusive
		false, // nowait
		amqp091.Table{
			"x-queue-type": "quorum",
		}, // args
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	// Bind the queue to the exchange with a routing key
	err = ch.QueueBind(
		q1.Name, // queue name
		"", // no need routing key cuz of fanout exchange
		"x.logs", // exchange name
		false, // no wait
		nil, // args
	)
	if err != nil {
		log.Fatalln("Failed to bind the queue 1 to the exchange:", err)
	}

	// Bind the queue to the exchange with a routing key
	err = ch.QueueBind(
		q2.Name, // queue name
		"", // no need routing key cuz of fanout exchange
		"x.logs", // exchange name
		false, // no wait
		nil, // args
	)
	if err != nil {
		log.Fatalln("Failed to bind the queue 2 to the exchange:", err)
	}


	log.Printf("Attempting to publish message")
	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ (type 'quit' to exit)")
	for {
		log.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read input:", err)
		}
		input = strings.TrimSpace(input)

		if strings.ToLower(input) == "quit" {
			log.Println("Exiting producer...")
			break
		}

		// Publish a message
		err = ch.Publish(
			"x.logs", // exchange name
			"", // routing key no need
			false, // mandatory
			false, // immediate
			amqp091.Publishing{
				ContentType: "text/plain",
				Body: []byte(input),
			},
		)
		if err != nil {
			log.Fatalln("Failed to publish a message:", err)
		}

		log.Println("Sent:", input)
	}

}
