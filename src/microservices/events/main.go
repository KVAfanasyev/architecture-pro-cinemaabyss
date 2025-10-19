package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"cinemaabyss-events/handlers"
	"cinemaabyss-events/kafka"
	"cinemaabyss-events/models"
)

func main() {
	// Получение переменных окружения
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	// Инициализация Kafka producer
	producer := kafka.NewProducer(kafkaBrokers)
	defer producer.Close()

	// Инициализация Kafka consumers
	movieConsumer := kafka.NewConsumer(kafkaBrokers, "movie-events", "events-service-group")
	defer movieConsumer.Close()

	userConsumer := kafka.NewConsumer(kafkaBrokers, "user-events", "events-service-group")
	defer userConsumer.Close()

	paymentConsumer := kafka.NewConsumer(kafkaBrokers, "payment-events", "events-service-group")
	defer paymentConsumer.Close()

	// Обработчик событий
	eventHandler := func(event *models.Event) {
		switch event.Type {
		case "movie":
			var movieEvent models.MovieEvent
			if err := json.Unmarshal(event.Payload, &movieEvent); err == nil {
				log.Printf("Processing movie event: %s - %s", movieEvent.Title, movieEvent.Action)
				// Здесь может быть бизнес-логика обработки события
			}
		case "user":
			var userEvent models.UserEvent
			if err := json.Unmarshal(event.Payload, &userEvent); err == nil {
				log.Printf("Processing user event: %d - %s", userEvent.UserID, userEvent.Action)
				// Здесь может быть бизнес-логика обработки события
			}
		case "payment":
			var paymentEvent models.PaymentEvent
			if err := json.Unmarshal(event.Payload, &paymentEvent); err == nil {
				log.Printf("Processing payment event: %d - %.2f - %s",
					paymentEvent.PaymentID, paymentEvent.Amount, paymentEvent.Status)
				// Здесь может быть бизнес-логика обработки события
			}
		}
	}

	// Запуск consumers
	ctx := context.Background()
	movieConsumer.StartConsuming(ctx, eventHandler)
	userConsumer.StartConsuming(ctx, eventHandler)
	paymentConsumer.StartConsuming(ctx, eventHandler)

	// Настройка HTTP маршрутов
	handler := handlers.NewEventHandler(producer)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handler.HealthCheck)
	mux.HandleFunc("/api/events/health", handler.HealthCheck)
	mux.HandleFunc("/api/events/movie", handler.CreateMovieEvent)
	mux.HandleFunc("/api/events/user", handler.CreateUserEvent)
	mux.HandleFunc("/api/events/payment", handler.CreatePaymentEvent)

	// Запуск HTTP сервера
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		log.Printf("Events service starting on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Ожидание сигнала для graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
