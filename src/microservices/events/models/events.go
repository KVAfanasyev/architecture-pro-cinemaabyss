package models

import (
	"encoding/json"
	"time"
)

// Event представляет базовую структуру события
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// MovieEvent представляет событие фильма
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

// UserEvent представляет событие пользователя
type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

// PaymentEvent представляет событие платежа
type PaymentEvent struct {
	PaymentID int       `json:"payment_id"`
	UserID    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Method    string    `json:"method_type,omitempty"`
}

// EventResponse представляет ответ при создании события
type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     *Event `json:"event"`
}

// HealthResponse представляет ответ для health check
type HealthResponse struct {
	Status bool `json:"status"`
}