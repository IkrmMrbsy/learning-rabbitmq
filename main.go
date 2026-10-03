package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go"
)

var rabbitChannel *amqp.Channel

type Task struct {
	Name string `json:"name"`
}

func taskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var task Task

	err := json.NewDecoder(r.Body).Decode(&task)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	fmt.Println("Task received:", task.Name)

	err = rabbitChannel.Publish(
		"",
		"tasks",
		false,
		false,
		amqp.Publishing{
			ContentType: "task/plain",
			Body:        []byte(task.Name),
		},
	)
	if err != nil {
		http.Error(w, "Failed to publish task", http.StatusInternalServerError)
		return
	}

	fmt.Println("Task sent to RabbitMQ!")

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, "Task accepted")
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ!")

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer channel.Close()

	rabbitChannel = channel

	fmt.Println("Channel created!")

	_, err = channel.QueueDeclare(
		"tasks",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Queue 'tasks' created!")

	http.HandleFunc("/tasks", taskHandler)

	fmt.Println("Server running on http://localhost:8080")

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println(err)
	}
}
