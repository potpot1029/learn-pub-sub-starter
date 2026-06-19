package main

import (
	"fmt"

	"github.com/potpot1029/learn-pub-sub-starter/internal/gamelogic"
	"github.com/potpot1029/learn-pub-sub-starter/internal/pubsub"
	"github.com/potpot1029/learn-pub-sub-starter/internal/routing"
)

func handlerLog() func(routing.GameLog) pubsub.Acktype {
	return func(gamelog routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			return pubsub.NackRequeue
		}

		return pubsub.Ack
	}
}
