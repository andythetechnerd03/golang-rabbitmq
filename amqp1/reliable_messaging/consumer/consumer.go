package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "q.amqp1.reliable"
	var received int32
	stateChanged := make(chan *rmq.StateChanged, 1)
	var wg sync.WaitGroup

	ctxB := context.Background()

	startTime := time.Now()

	rmq.Info("Creating reliable connection to RabbitMQ")

	conn, err := rmq.Dial(ctxB, "amqp://", &rmq.AmqpConnOptions{
		ContainerID: "reliable-producer",
		RecoveryConfiguration: &rmq.RecoveryConfiguration{
			ActiveRecovery: true,
			BackOffReconnectInterval: 2 * time.Second,
			MaxReconnectAttempts: 5,
		},		
	})
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
	q, err := management.DeclareQueue(ctxB, &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rmq.Error("Failed to declare queue: %v", err)
		return
	}

	consumer, err := conn.NewConsumer(ctxB, q.Name(), nil)
	if err != nil {
		rmq.Error("Failed to create consumer: %v", err)
		return
	}

	ctxC, cancel := context.WithCancel(ctxB)
	defer cancel()
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			deliveryContext, err := consumer.Receive(ctxC)
			if errors.Is(err, context.Canceled) {
				rmq.Info("Context canceled, stopping consumer")
				break
			} else if err != nil {
				rmq.Error("Failed to receive message: %v", err)
				continue
			}
			atomic.AddInt32(&received, 1)

			// rmq.Info("Received message: ", fmt.Sprintf("%s", deliveryContext.Message().Data))
			err = deliveryContext.Accept(ctxB)
			if err != nil {
				rmq.Error("Failed to accept message: %v", err)
			}

		
		}
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				mps := float64(received) / float64(time.Since(startTime).Seconds())
				log.Println("[Stats]", "Received:", received, "Message Rate:", mps)
			case <-ctxC.Done():
				rmq.Info("Context canceled, stopping stats logger")
				return
			}
		}
	}(ctxC)


	fmt.Println("Waiting for incoming messages. Press enter to close the consumer")
	_, err = fmt.Scanln()
	if err != nil {
		rmq.Error("Failed to read input: %v", err)
		return
	}

	cancel()

	err = consumer.Close(ctxB)
	if err != nil {
		rmq.Error("Failed to close consumer: %v", err)
		return
	}

	// err = env.CloseConnections(ctxB)
	err = conn.Close(ctxB)
	if err != nil {
		rmq.Error("Failed to close connections: %v", err)
		return
	}



	wg.Wait()
	close(stateChanged)

}