package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	processingSeconds := 5

	if value := os.Getenv("PROCESSING_SECONDS"); value != "" {
		seconds, err := strconv.Atoi(value)
		if err != nil || seconds < 1 {
			panic("PROCESSING_SECONDS harus berupa angka positif")
		}

		processingSeconds = seconds
	}

	processingDuration := time.Duration(processingSeconds) * time.Second

	fmt.Printf("Processing duration: %d seconds\n", processingSeconds)

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

	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		queueName = "email-tasks"
	}

	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Worker consuming queue:", queue.Name)

	err = channel.Qos(1, 0, false)
	if err != nil {
		panic(err)
	}

	messages, err := channel.Consume(
		queue.Name,
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

		time.Sleep(processingDuration)

		fmt.Println("Task finished!")

		err := message.Ack(false)
		if err != nil {
			fmt.Println("Failed to ACK:", err)
			continue
		}

		fmt.Println("Task ACK sent!")
	}
}
