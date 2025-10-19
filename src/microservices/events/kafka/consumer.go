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
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) StartConsuming(ctx context.Context, handler func(event *models.Event)) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := c.reader.ReadMessage(ctx)
				if err != nil {
					log.Printf("Error reading message from kafka: %v", err)
					continue
				}

				var event models.Event
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					log.Printf("Error unmarshaling event: %v", err)
					continue
				}

				log.Printf("Received event: ID=%s, Type=%s, Topic=%s, Partition=%d, Offset=%d",
					event.ID, event.Type, msg.Topic, msg.Partition, msg.Offset)

				handler(&event)
			}
		}
	}()
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
