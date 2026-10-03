package main

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Worker connected to RabbitMQ!")

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer channel.Close()

	fmt.Println("Worker channel created!")

	messages, err := channel.Consume(
		"tasks",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Worker waiting for tasks...")

	for message := range messages {
		fmt.Println("Task received by worker:", string(message.Body))

		fmt.Println("Processing task...")

		time.Sleep(5 * time.Second)

		// fmt.Println("Task finished!")

		err := message.Ack(false)
		if err != nil {
			fmt.Println("Failed to ACK:", err)
			continue
		}

		fmt.Println("Task ACK sent!")
	}
}
