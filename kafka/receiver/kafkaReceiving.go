package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0 // Kafka 3.5.x와 호환

	// Kafka 설정 및 클라이언트 생성
	group := "my-group"
	brokers := []string{"localhost:9092"}
	topics := []string{"hello"}

	consumer := Consumer{
		ready: make(chan bool), // 채널을 초기화합니다.
	}
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		fmt.Printf("Failed to create consumer group: %s\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// 시그널 처리를 위한 채널
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				fmt.Printf("Error during consumption: %v\n", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // 컨슈머가 준비될 때까지 대기
	fmt.Println("Sarama consumer up and running!")

	select {
	case sig := <-sigchan:
		fmt.Printf("Caught signal %v: terminating\n", sig)
		cancel()
	}

	wg.Wait()
	fmt.Println("Consumer closed.")
}

// Consumer 구조체 정의
type Consumer struct {
	ready chan bool
}

// Setup은 컨슈머가 준비된 후 호출됩니다.
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// `consumer.ready`가 nil인지 확인하지 않고 닫기 전에 생성되었는지 확인
	if consumer.ready != nil {
		close(consumer.ready)
	}
	return nil
}

// Cleanup은 세션이 종료될 때 호출됩니다.
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim은 실제 메시지를 소비합니다.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Received message: %s\n", string(message.Value))
		session.MarkMessage(message, "")
	}
	return nil
}
