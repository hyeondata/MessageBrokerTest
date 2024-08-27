package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
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
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // All replicas must acknowledge
	config.Producer.Retry.Max = 5                    // Retry up to 5 times to produce a message
	config.Producer.Return.Successes = true          // Return success message after send
	config.Producer.Return.Errors = true             // Return error message after send

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	defer producer.Close()

	return send(producer)
}

func send(producer sarama.SyncProducer) error {
	topic := "hello"
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

	// Gracefully handle shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				message := &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.StringEncoder("Hello World!"),
				}

				_, _, err := producer.SendMessage(message)
				if err != nil {
					logError(err, "Failed to produce message")
					return
				}
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

		case <-signals:
			log.Println("Interrupt is detected")
			cancel()
			return nil
		}
	}
}
