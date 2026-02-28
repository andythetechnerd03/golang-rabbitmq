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

	err = ch.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare an exchange:", err)
	}

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

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ (type 'quit' to exit)")
	log.Println("Routing keys option: error, info")
	for {
		log.Print("Enter routing key (error/info): ")
		routingKey, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read input:", err)
		}
		routingKey = strings.TrimSpace(strings.ToLower(routingKey))

		if routingKey == "quit" {
			log.Println("Exiting producer...")
			break
		}
		
		if routingKey != queues[0].key && routingKey != queues[1].key {
			log.Println("Invalid routing key. Please use 'error' or 'info'.")
			continue
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
			routingKey, // routing key is set to the user input for direct exchange
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