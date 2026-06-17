package main

import (
	"fmt"
	"log"

	"github.com/potpot1029/learn-pub-sub-starter/internal/gamelogic"
	"github.com/potpot1029/learn-pub-sub-starter/internal/pubsub"
	"github.com/potpot1029/learn-pub-sub-starter/internal/routing"
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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("error creating a new channel on the connection: %v", err)
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.DurableQueue,
	)

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("publishing a pause message...")
			err := pubsub.PublishJSON(ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("error publishing pause message: %v", err)
			}
		case "resume":
			fmt.Println("publishing a resume message...")
			err := pubsub.PublishJSON(ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("error publishing resume message: %v", err)
			}
		case "quit":
			fmt.Println("quitting...")
			return
		default:
			fmt.Println("unknown command")
		}
	}

}
