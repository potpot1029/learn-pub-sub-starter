package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype string

const (
	Ack         Acktype = "ack"
	NackRequeue Acktype = "nack_requeue"
	NackDiscard Acktype = "nack_discard"
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("[subscribe] error declaring and binding queue to exchange: %v", err)
	}

	err = ch.Qos(10, 0, true)
	if err != nil {
		return fmt.Errorf("[subscribe] error prefetching messages: %v", err)
	}

	deliverCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("[subscribe] error consuming queued messages: %v", err)
	}

	go func() {
		for msg := range deliverCh {

			body, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("[subscribe] error unmarhsaling message body: %v", err)
				return
			}

			acktype := handler(body)

			switch acktype {
			case Ack:
				fmt.Println("acknowledging message...")
				if err = msg.Ack(false); err != nil {
					fmt.Printf("[subscribe] error acknowledging message: %v", err)
					return
				}
			case NackRequeue:
				fmt.Println("nack and requeuing message...")
				if err = msg.Nack(false, true); err != nil {
					fmt.Printf("[subscribe] error nack and requeuing message: %v", err)
					return
				}
			case NackDiscard:
				fmt.Println("nack and discarding message...")
				if err = msg.Nack(false, false); err != nil {
					fmt.Printf("[subscribe] error nack and discarding message: %v", err)
					return
				}
			}
		}

	}()

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var val T
			decoder := json.NewDecoder(bytes.NewBuffer(data))

			err := decoder.Decode(&val)
			if err != nil {
				return val, err
			}

			return val, nil
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var val T
			decoder := gob.NewDecoder(bytes.NewBuffer(data))

			err := decoder.Decode(&val)
			if err != nil {
				return val, err
			}

			return val, nil
		},
	)
}
