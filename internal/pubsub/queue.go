package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	DurableQueue   SimpleQueueType = "durable"
	TransientQueue SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("[DeclareAndBind] error creating a new channel: %v", err)
	}

	var durable, autoDelete, exclusive bool
	if queueType == DurableQueue {
		durable, autoDelete, exclusive = true, false, false
	} else if queueType == TransientQueue {
		durable, autoDelete, exclusive = false, true, true
	}
	noWait := false

	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("[DeclareAndBind] error creating a queue: %v", err)
	}

	err = ch.QueueBind(queueName, key, exchange, noWait, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("[DeclareAndBind] error binding a queue: %v", err)
	}

	return ch, queue, nil
}
