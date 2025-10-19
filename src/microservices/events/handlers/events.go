package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cinemaabyss-events/kafka"
	"cinemaabyss-events/models"
)

type EventHandler struct {
	producer *kafka.Producer
}

func NewEventHandler(producer *kafka.Producer) *EventHandler {
	return &EventHandler{
		producer: producer,
	}
}

// HealthCheck обрабатывает проверку работоспособности
func (h *EventHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := models.HealthResponse{Status: true}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CreateMovieEvent создает событие фильма
func (h *EventHandler) CreateMovieEvent(w http.ResponseWriter, r *http.Request) {
	var movieEvent models.MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Валидация обязательных полей
	if movieEvent.MovieID == 0 || movieEvent.Title == "" || movieEvent.Action == "" {
		http.Error(w, "Missing required fields: movie_id, title, action", http.StatusBadRequest)
		return
	}

	partition, offset, err := h.producer.SendMovieEvent(r.Context(), &movieEvent)
	if err != nil {
		log.Printf("Failed to send movie event to kafka: %v", err)
		http.Error(w, "Failed to create event", http.StatusInternalServerError)
		return
	}

	// Создаем ответ
	kafkaEvent := &models.Event{
		ID:        fmt.Sprintf("movie-%d-%s", movieEvent.MovieID, movieEvent.Action),
		Type:      "movie",
		Timestamp: time.Now(),
	}

	eventBytes, _ := json.Marshal(movieEvent)
	kafkaEvent.Payload = eventBytes

	response := models.EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     kafkaEvent,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// CreateUserEvent создает событие пользователя
func (h *EventHandler) CreateUserEvent(w http.ResponseWriter, r *http.Request) {
	var userEvent models.UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Валидация обязательных полей
	if userEvent.UserID == 0 || userEvent.Action == "" {
		http.Error(w, "Missing required fields: user_id, action", http.StatusBadRequest)
		return
	}

	// Если timestamp не указан, устанавливаем текущее время
	if userEvent.Timestamp.IsZero() {
		userEvent.Timestamp = time.Now()
	}

	partition, offset, err := h.producer.SendUserEvent(r.Context(), &userEvent)
	if err != nil {
		log.Printf("Failed to send user event to kafka: %v", err)
		http.Error(w, "Failed to create event", http.StatusInternalServerError)
		return
	}

	// Создаем ответ
	kafkaEvent := &models.Event{
		ID:        fmt.Sprintf("user-%d-%s", userEvent.UserID, userEvent.Action),
		Type:      "user",
		Timestamp: userEvent.Timestamp,
	}

	eventBytes, _ := json.Marshal(userEvent)
	kafkaEvent.Payload = eventBytes

	response := models.EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     kafkaEvent,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// CreatePaymentEvent создает событие платежа
func (h *EventHandler) CreatePaymentEvent(w http.ResponseWriter, r *http.Request) {
	var paymentEvent models.PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Валидация обязательных полей
	if paymentEvent.PaymentID == 0 || paymentEvent.UserID == 0 || paymentEvent.Amount == 0 || paymentEvent.Status == "" {
		http.Error(w, "Missing required fields: payment_id, user_id, amount, status", http.StatusBadRequest)
		return
	}

	// Если timestamp не указан, устанавливаем текущее время
	if paymentEvent.Timestamp.IsZero() {
		paymentEvent.Timestamp = time.Now()
	}

	partition, offset, err := h.producer.SendPaymentEvent(r.Context(), &paymentEvent)
	if err != nil {
		log.Printf("Failed to send payment event to kafka: %v", err)
		http.Error(w, "Failed to create event", http.StatusInternalServerError)
		return
	}

	// Создаем ответ
	kafkaEvent := &models.Event{
		ID:        fmt.Sprintf("payment-%d-%s", paymentEvent.PaymentID, paymentEvent.Status),
		Type:      "payment",
		Timestamp: paymentEvent.Timestamp,
	}

	eventBytes, _ := json.Marshal(paymentEvent)
	kafkaEvent.Payload = eventBytes

	response := models.EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     kafkaEvent,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}
