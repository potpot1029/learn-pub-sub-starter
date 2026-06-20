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

	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(connUrl)
	if err != nil {
		log.Fatalf("error starting Peril client: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected Peril client!")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("error creating a new channel on the connection: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error getting username: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("error subscribing to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.TransientQueue,
		handlerMove(gs, ch),
	)
	if err != nil {
		log.Fatalf("error subscribing to move: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.DurableQueue,
		handlerWar(gs, ch),
	)
	if err != nil {
		log.Fatalf("error subscribing to war: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				log.Printf("error processing spawn command: %v", err)
			}
		case "move":
			move, err := gs.CommandMove(words)
			if err != nil {
				log.Printf("error processing move command: %v", err)
				continue
			}

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				move,
			)
			if err != nil {
				log.Printf("error publishing move message: %v", err)
				continue
			}

			log.Println("move message published successfully!")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			err := gs.CommandSpam(ch, words)
			if err != nil {
				log.Printf("error processing spam command: %v", err)
				continue
			}

		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}
