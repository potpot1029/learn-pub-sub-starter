package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("[PublishGob] error encoding val: %v", err)
	}

	ctx := context.Background()
	mandatory, immediate := false, false
	err = ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	})
	if err != nil {
		return fmt.Errorf("[PublishGob] error publishing to exchange: %v", err)
	}

	return nil
}
