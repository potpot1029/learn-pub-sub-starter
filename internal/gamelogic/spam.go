package gamelogic

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/potpot1029/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (gs *GameState) CommandSpam(ch *amqp.Channel, words []string) error {
	if len(words) < 2 {
		return errors.New("usage: spamj <n>")
	}

	n, err := strconv.Atoi(words[1])
	if err != nil {
		return fmt.Errorf("error: %s cannot be parsed into integer properly", words[1])
	}

	maliciousLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     GetMaliciousLog(),
		Username:    gs.Player.Username,
	}

	for range n {
		err := PublishGameLog(ch, maliciousLog)
		if err != nil {
			return fmt.Errorf("error when spamming messages: %v", err)
		}
	}

	return nil
}
