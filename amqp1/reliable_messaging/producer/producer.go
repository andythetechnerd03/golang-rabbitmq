package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "q.amqp1.reliable"
	stateChanged := make(chan *rmq.StateChanged, 1)
	var wg sync.WaitGroup

	ctx := context.Background()

	var stateAccepted int32
	var stateReleased int32
	var stateRejected int32
	var stateModified int32
	var failed int32 

	startTime := time.Now()

	rmq.Info("Creating reliable messaging connection to RabbitMQ")

	conn, err := rmq.Dial(ctx, "amqp://", &rmq.AmqpConnOptions{
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
			if statusChanged.String() == "From: open, To: closed, Error: %!s(<nil>)" {
				fmt.Println("Connection closed, stopping producer...")
				return
			}
		}
	}()

	publisher, err := conn.NewPublisher(ctx, &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		rmq.Error("Failed to create publisher: %v", err)
		return
	}

	for i := 0; i < 100_000; i++{


		publishResult, err := publisher.Publish(ctx, rmq.NewMessage([]byte("Hello, AMQP 1.0! " + fmt.Sprint("%d", i))))
		if err != nil {
			atomic.AddInt32(&failed, 1)
			log.Fatalln("Failed to publish message:", err)
			continue
		}
		// rmq.Info("Publish result: %s", publishResult)

		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			atomic.AddInt32(&stateAccepted, 1)
			rmq.Info("Message accepted")
		case *rmq.StateRejected:
			atomic.AddInt32(&stateRejected, 1)
			rmq.Info("Message rejected")
		case *rmq.StateReleased:
			atomic.AddInt32(&stateReleased, 1)
			rmq.Info("Message released")
		case *rmq.StateModified:
			atomic.AddInt32(&stateModified, 1)
			rmq.Info("Message modified")
		default:
			rmq.Info("Unknown publish outcome")
		}

	}

	err = publisher.Close(ctx)
	if err != nil {
		rmq.Error("Failed closing publisher: %v", err)
		return
	}

	mps := float64(stateAccepted + stateRejected + stateReleased + stateModified) / float64(time.Since(startTime).Seconds())
	fmt.Println("[*Stats*]", "sent:", stateAccepted + stateRejected + stateReleased + stateModified, "failed:", failed, "Message Rate:", mps)
	fmt.Println("[*Stats*]", "accepted:", stateAccepted, "rejected:", stateRejected, "released:", stateReleased, "modified:", stateModified)

	err = conn.Close(ctx)
	if err != nil {
		rmq.Error("Failed closing connection: %v", err)
		return
	}

	wg.Wait()
	close(stateChanged)

}