package kafka

import (
	"encoding/json"
	"fmt"
	"time"
	"github.com/segmentio/kafka-go"
	"cinemaabyss-events/models"
	"context"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	return &Producer{
		writer: writer,
	}
}

func (p *Producer) SendMovieEvent(ctx context.Context, event *models.MovieEvent) (int, int64, error) {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal movie event: %w", err)
	}

	kafkaEvent := &models.Event{
		ID:        fmt.Sprintf("movie-%d-%s", event.MovieID, event.Action),
		Type:      "movie",
		Timestamp: time.Now(),
		Payload:   eventBytes,
	}

	return p.sendEvent(ctx, "movie-events", kafkaEvent)
}

func (p *Producer) SendUserEvent(ctx context.Context, event *models.UserEvent) (int, int64, error) {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal user event: %w", err)
	}

	kafkaEvent := &models.Event{
		ID:        fmt.Sprintf("user-%d-%s", event.UserID, event.Action),
		Type:      "user",
		Timestamp: time.Now(),
		Payload:   eventBytes,
	}

	return p.sendEvent(ctx, "user-events", kafkaEvent)
}

func (p *Producer) SendPaymentEvent(ctx context.Context, event *models.PaymentEvent) (int, int64, error) {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal payment event: %w", err)
	}

	kafkaEvent := &models.Event{
		ID:        fmt.Sprintf("payment-%d-%s", event.PaymentID, event.Status),
		Type:      "payment",
		Timestamp: time.Now(),
		Payload:   eventBytes,
	}

	return p.sendEvent(ctx, "payment-events", kafkaEvent)
}

func (p *Producer) sendEvent(ctx context.Context, topic string, event *models.Event) (int, int64, error) {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal event: %w", err)
	}

	message := kafka.Message{
		Topic: topic,
		Key:   []byte(event.ID),
		Value: eventBytes,
		Time:  time.Now(),
	}

	err = p.writer.WriteMessages(ctx, message)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write message to kafka: %w", err)
	}

	// В реальном приложении partition и offset были бы получены из ответа Kafka
	// Для MVP возвращаем заглушки
	return 0, 42, nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
