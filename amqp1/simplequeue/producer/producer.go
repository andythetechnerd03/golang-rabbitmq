package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	exchangeName := "x.amqp1.exchange"
	queueName := "q.amqp1"
	routingKey := "amqp1"
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
	_, err = management.DeclareExchange(ctx, &rmq.DirectExchangeSpecification{
		Name: exchangeName,
	})
	if err != nil {
		rmq.Error("Failed to declare exchange: %v", err)
		return
	}

	_, err = management.DeclareQueue(ctx, &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rmq.Error("Failed to declare queue: %v", err)
		return
	}

	bindingString, err := management.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
		SourceExchange: exchangeName,
		DestinationQueue: queueName,
		BindingKey: routingKey,
	})
	if err != nil {
		rmq.Error("Failed to bind queue to exchange: %v", err)
		return
	}
	rmq.Info("Binding string: %s", bindingString)

	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{
		Exchange: exchangeName,
		Key: routingKey,
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

		publishResult, err := publisher.Publish(ctx, rmq.NewMessage([]byte(input)))
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