package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"cinemaabyss-events/models"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers string, topic string, groupID string) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           []string{brokers},
		Topic:             topic,
		GroupID:           groupID,
		MinBytes:          10e3,             // 10KB
		MaxBytes:          10e6,             // 10MB
		CommitInterval:    10 * time.Second, // Увеличить с 1 до 10 секунд
		StartOffset:       kafka.FirstOffset,
		MaxWait:           30 * time.Second,       // Добавить
		ReadBackoffMin:    100 * time.Millisecond, // Добавить
		ReadBackoffMax:    1 * time.Second,        // Добавить
		SessionTimeout:    30 * time.Second,       // Добавить
		HeartbeatInterval: 10 * time.Second,       // Добавить
	})

	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) StartConsuming(ctx context.Context, handler func(event *models.Event)) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("Stopping consumer for topic: %s", c.reader.Config().Topic)
				return
			default:
				msg, err := c.reader.ReadMessage(ctx)
				if err != nil {
					if err == context.Canceled {
						log.Printf("Consumer context canceled for topic: %s", c.reader.Config().Topic)
						return
					}
					log.Printf("Error reading message from kafka topic %s: %v", c.reader.Config().Topic, err)
					// Добавляем задержку перед повторной попыткой
					time.Sleep(2 * time.Second)
					continue
				}

				var event models.Event
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					log.Printf("Error unmarshaling event from topic %s: %v", c.reader.Config().Topic, err)
					continue
				}

				log.Printf("Received event: ID=%s, Type=%s, Topic=%s, Partition=%d, Offset=%d",
					event.ID, event.Type, msg.Topic, msg.Partition, msg.Offset)

				// Обработка в отдельной goroutine для избежания блокировки
				go handler(&event)
			}
		}
	}()

	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
