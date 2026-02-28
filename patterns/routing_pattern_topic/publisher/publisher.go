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

	exchangeName := "x.topic.logs"

	err = ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare an exchange:", err)
	}

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ (type 'quit' to exit)")
	for {
		log.Printf("Enter routing key: ")
		routingKey, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read routing key:", err)
		}
		routingKey = strings.TrimSpace(strings.ToLower(routingKey))

		if routingKey == "quit" {
			log.Println("Exiting producer...")
			break
		}

		log.Print("Message: ")
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
			routingKey, // routing key is set to the user input for topic exchange
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