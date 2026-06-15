package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connUrl = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(connUrl)
	if err != nil {
		log.Fatalf("error starting Peril server: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to Peril server!")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Shutting down Peril server...")
}
