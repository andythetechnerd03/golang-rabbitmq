package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {

	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetHost("localhost").SetPort(5552).SetUser("guest").SetPassword("guest"))
	if err != nil {
		log.Fatalln("Failed to create environment:", err)
	}
	defer env.Close()

	streamName := "stream.hello"
	err = env.DeclareStream(streamName, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(2),
	})
	if err != nil {
		log.Fatalln("Failed to declare stream:", err)
	}

	// Create a consumer to receive messages from the stream
	// Explain: The messagesHandler function will be called whenever a new message is received from the stream. It simply prints the stream name and the message content to the console.
	messagesHandler := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("Stream: %s - Received message: %s\n", consumerContext.Consumer.GetStreamName(), message.Data)
	}

	// Explain: We set the offset to 'first' to consume all existing messages in the stream. If we wanted to consume only new messages, we could set it to 'last'.
	// Note: The consumer will keep running and receiving messages until the program is terminated. In a real application, you would likely want to handle graceful shutdowns and other edge cases.
	consumerOptions := stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First())

	// Explain: We create a consumer for the stream using the environment we created earlier. We pass in the stream name, the message handler function, and the consumer options. If there is an error creating the consumer, we log it and exit.
	// We also defer the closing of the consumer to ensure it is properly cleaned up when the program exits.
	consumer, err := env.NewConsumer(streamName, messagesHandler, consumerOptions)
	if err != nil {
		log.Fatalln("Failed to create consumer:", err)
	}
	defer consumer.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Waiting for incoming messages. Press enter to close the consumer")
	_, err = reader.ReadString('\n')
	if err != nil {
		log.Fatalln("Failed to read input:", err)
	}

	err = consumer.Close()
	if err != nil {
		log.Fatalln("Failed to close consumer:", err)
	}


}
