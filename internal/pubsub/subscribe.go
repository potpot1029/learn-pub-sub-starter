package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("[SubscribeJSON] error declaring and binding queue to exchange: %v", err)
	}

	deliverCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("[SubscribeJSON] error consuming queued messages: %v", err)
	}

	go func() {
		for msg := range deliverCh {

			var body T
			if err = json.Unmarshal(msg.Body, &body); err != nil {
				fmt.Printf("[SubscribeJSON] error unmarhsaling message body: %v", err)
				return
			}

			handler(body)

			if err = msg.Ack(false); err != nil {
				fmt.Printf("[SubscribeJSON] error acknowledging message: %v", err)
				return
			}
		}

	}()

	return nil
}
