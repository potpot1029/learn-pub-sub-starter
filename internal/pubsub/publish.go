package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	dat, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("[PublishJSON] error marshaling val: %v", err)
	}

	ctx := context.Background()
	mandatory, immediate := false, false
	err = ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, amqp.Publishing{
		ContentType: "application/json",
		Body:        dat,
	})
	if err != nil {
		return fmt.Errorf("[PublishJSON] error publishing to exchange: %v", err)
	}

	return nil
}
