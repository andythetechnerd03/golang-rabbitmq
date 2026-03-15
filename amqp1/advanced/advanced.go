package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	fmt.Println("Go AMQP 1.0 Advanced Connection settings")

	env := rmq.NewClusterEnvironmentWithStrategy([]rmq.Endpoint{
		{
			Address: "amqp://localhost:5674",
			Options: &rmq.AmqpConnOptions{
				Id: "wrong container config",
				RecoveryConfiguration: &rmq.RecoveryConfiguration{
					ActiveRecovery: true,
					BackOffReconnectInterval: 2 * time.Second,
					MaxReconnectAttempts: 5,
				},
				SASLType: amqp.SASLTypePlain("guest", "guest"),
			},
		},
		{
			Address: "amqp://localhost:5673",
			Options: &rmq.AmqpConnOptions{
				SASLType: amqp.SASLTypeAnonymous(),
				RecoveryConfiguration: &rmq.RecoveryConfiguration{
					ActiveRecovery: false,
				},
				Id: "container one",
			},
		},
		{
			Address: "amqp://localhost:5672",
			Options: &rmq.AmqpConnOptions{
				SASLType: amqp.SASLTypePlain("guest", "guest"),
				RecoveryConfiguration: &rmq.RecoveryConfiguration{
					ActiveRecovery: true,
					BackOffReconnectInterval: 2 * time.Second,
					MaxReconnectAttempts: 5,
				},
				Id: "container two",
			},
			},
	}, rmq.StrategySequential)
	
	for range 5 {
		conn, err := env.NewConnection(context.Background())
		if err != nil {
			fmt.Printf("Error creating connection: %v\n", err)
			continue
		}
		fmt.Printf("Connection created: %v\n", conn.Properties())
		fmt.Printf("Connected ID: %s\n", conn.Id())
		fmt.Printf("Connection state: %+v\n", conn.State())
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Println("Press any key to close connections...")
	fmt.Scanln()

	
}