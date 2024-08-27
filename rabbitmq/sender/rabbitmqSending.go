package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func logError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func main() {
	for {
		if err := run(); err != nil {
			log.Printf("Error occurred: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}
}

func run() error {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	return send(conn)
}

func send(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logFile, err := os.Create("message_rate.log")
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}
	defer logFile.Close()

	messageCount := 0
	ticker := time.NewTicker(1 * time.Minute)
	startTime := time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:

				body := "Hello World!"
				err = ch.PublishWithContext(ctx,
					"",     // exchange
					q.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					})
				if err != nil {
					logError(err, "Failed to publish a message")
					return
				}
				// log.Printf(" [x] Sent %s\n", body)
				messageCount++
			}
		}
	}()

	for {
		select {
		case <-ticker.C:
			rate := float64(messageCount) / time.Since(startTime).Minutes()
			logEntry := fmt.Sprintf("Time: %s, Messages sent in last minute: %d, Average rate: %.2f messages/minute\n",
				time.Now().Format("2006-01-02 15:04:05"), messageCount, rate)

			_, err := logFile.WriteString(logEntry)
			if err != nil {
				log.Printf("Error writing to log file: %v", err)
			}

			log.Print(logEntry)
			messageCount = 0
			startTime = time.Now()
		case <-conn.NotifyClose(make(chan *amqp.Error)):
			return fmt.Errorf("connection closed")
		}
	}
}
