package main

import (
	"log"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	// RabbitMQ 서버에 연결
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// 채널 열기
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Exchange 선언
	err = ch.ExchangeDeclare(
		"logstash_exchange", // exchange name
		"fanout",            // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 큐 선언
	q, err := ch.QueueDeclare(
		"logstash_queue", // 큐 이름 (큐는 고유 이름을 사용할 수 있습니다)
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 큐와 Exchange 바인딩 (fanout 타입이므로 routing key가 필요 없음)
	err = ch.QueueBind(
		q.Name,              // 큐 이름
		"",                  // routing key (fanout 타입은 사용하지 않음)
		"logstash_exchange", // exchange 이름
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// 큐에서 메시지 소비
	consume(ch, q)
}

func consume(ch *amqp.Channel, q amqp.Queue) {
	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}
	var messageCount uint64

	go func() {
		for d := range msgs {
			count := atomic.AddUint64(&messageCount, 1)
			log.Printf("Received message: body=%s, messageCount=%d", d.Body, count)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
