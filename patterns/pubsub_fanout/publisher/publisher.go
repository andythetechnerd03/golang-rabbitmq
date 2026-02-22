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

	exchangeName := "x.logs"

	err = ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare an exchange:", err)
	}

	q, err := ch.QueueDeclare("queue1", true, false, false, false, amqp091.Table{
		"x-queue-type": "quorum",
	})
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	q2, err := ch.QueueDeclare("queue2", true, false, false, false, amqp091.Table{
		"x-queue-type": "quorum",
	})
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	err = ch.QueueBind(q.Name, "", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind a queue:", err)
	}

	err = ch.QueueBind(q2.Name, "", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind a queue:", err)
	}

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
			exchangeName, // exchange name
			"", // routing key is empty for fanout exchange
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