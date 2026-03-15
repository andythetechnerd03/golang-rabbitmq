package main

import (
	"bufio"
	"context"
	"math/rand"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "q.amqp1.stream"
	stateChanged := make(chan *rmq.StateChanged, 1)
	var wg sync.WaitGroup

	ctx := context.Background()

	rmq.Info("Creating connection to RabbitMQ")

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)
	
	conn, err := env.NewConnection(ctx)
	if err != nil {
		rmq.Error("Failed to create connection: %v", err)
		return
	}
	conn.NotifyStatusChange(stateChanged)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for statusChanged := range stateChanged {
			fmt.Printf("Connection status changed: %s\n", statusChanged)

		}
	}()

	management := conn.Management()

	_, err = management.DeclareQueue(ctx, &rmq.StreamQueueSpecification{
		Name: queueName,
		MaxLengthBytes: rmq.CapacityGB(3),
	})
	if err != nil {
		rmq.Error("Failed to declare queue: %v", err)
		return
	}

	publisher, err := conn.NewPublisher(ctx, &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		rmq.Error("Failed to create publisher: %v", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ AMQP 1.0 (type 'quit' to exit)")
	for {
		log.Print("Enter message: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read input:", err)
		}
		input = strings.TrimSpace(input)

		if strings.ToLower(input) == "quit" {
			log.Println("Exiting producer...")
			break
		}

		filters := []string{"filter1", "filter2", "filter3", "filter4", "filter5"}

		index := rand.Int31n(5)
		msg := rmq.NewMessageWithFilter([]byte(input), filters[index])
		publishResult, err := publisher.Publish(ctx, msg)
		if err != nil {
			log.Fatalln("Failed to publish message:", err)
			continue
		}
		// rmq.Info("Publish result: %s", publishResult)

		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("Message accepted")
		case *rmq.StateRejected:
			rmq.Info("Message rejected")
		case *rmq.StateReleased:
			rmq.Info("Message released")
		case *rmq.StateModified:
			rmq.Info("Message modified")
		default:
			rmq.Info("Unknown publish outcome")
		}

	}

	err = publisher.Close(ctx)
	if err != nil {
		rmq.Error("Failed to close publisher: %v", err)
		return
	}

	err = env.CloseConnections(ctx)
	if err != nil {
		rmq.Error("Failed to close connections: %v", err)
		return
	}

	wg.Wait()
	close(stateChanged)
}
