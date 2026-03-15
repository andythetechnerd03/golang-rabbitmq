package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "q.amqp1.stream"
	stateChanged := make(chan *rmq.StateChanged, 1)
	var wg sync.WaitGroup

	ctxB := context.Background()

	rmq.Info("Creating connection to RabbitMQ")

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)
	
	conn, err := env.NewConnection(ctxB)
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

	fmt.Println("Coding with nvim")

	consumer, err := conn.NewConsumer(ctxB, queueName, &rmq.StreamConsumerOptions{
		Offset: &rmq.OffsetNext{},
	})
	if err != nil {
		rmq.Error("Failed to create consumer: %v", err)
		return
	}

	ctxBC, cancel := context.WithCancel(ctxB)
	defer cancel()
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			deliveryContext, err := consumer.Receive(ctxBC)
			if errors.Is(err, context.Canceled) {
				rmq.Info("Context canceled, stopping consumer")
				break
			} else if err != nil {
				rmq.Error("Failed to receive message: %v", err)
				continue
			}

			rmq.Info("Received message: ", fmt.Sprintf("%s", deliveryContext.Message().Data))
			err = deliveryContext.Accept(ctxB)
			if err != nil {
				rmq.Error("Failed to accept message: %v", err)
			}

		
		}
	}()


	fmt.Println("Waiting for incoming messages. Press enter to close the consumer")
	fmt.Scanln()

	cancel()

	err = consumer.Close(ctxB)
	if err != nil {
		rmq.Error("Failed to close consumer: %v", err)
		return
	}

	err = env.CloseConnections(ctxB)
	if err != nil {
		rmq.Error("Failed to close connections: %v", err)
		return
	}

	wg.Wait()
	close(stateChanged)

}
