package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"cinemaabyss-events/handlers"
	"cinemaabyss-events/kafka"
	"cinemaabyss-events/models"
)

func main() {
	// Получение переменных окружения
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8082"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	// Инициализация Kafka producer
	producer := kafka.NewProducer(kafkaBrokers)
	defer producer.Close()

	// Инициализация Kafka consumers
	consumers := []*kafka.Consumer{
		kafka.NewConsumer(kafkaBrokers, "movie-events", "events-service-group"),
		kafka.NewConsumer(kafkaBrokers, "user-events", "events-service-group"),
		kafka.NewConsumer(kafkaBrokers, "payment-events", "events-service-group"),
	}

	// Обработчик событий
	eventHandler := func(event *models.Event) {
		switch event.Type {
		case "movie":
			var movieEvent models.MovieEvent
			if err := json.Unmarshal(event.Payload, &movieEvent); err == nil {
				log.Printf("Processing movie event: %s - %s", movieEvent.Title, movieEvent.Action)
				// Бизнес-логика обработки события
			}
		case "user":
			var userEvent models.UserEvent
			if err := json.Unmarshal(event.Payload, &userEvent); err == nil {
				log.Printf("Processing user event: %d - %s", userEvent.UserID, userEvent.Action)
				// Бизнес-логика обработки события
			}
		case "payment":
			var paymentEvent models.PaymentEvent
			if err := json.Unmarshal(event.Payload, &paymentEvent); err == nil {
				log.Printf("Processing payment event: %d - %.2f - %s",
					paymentEvent.PaymentID, paymentEvent.Amount, paymentEvent.Status)
				// Бизнес-логика обработки события
			}
		}
	}

	// Запуск consumers с контекстом
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, consumer := range consumers {
		wg.Add(1)
		go func(c *kafka.Consumer) {
			defer wg.Done()
			if err := c.StartConsuming(ctx, eventHandler); err != nil {
				log.Printf("Failed to start consumer: %v", err)
			}
		}(consumer)
	}

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
		ReadTimeout:  30 * time.Second, // Увеличить таймауты
		WriteTimeout: 30 * time.Second,
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

	// Останавливаем consumers
	cancel()

	// Ждем завершения consumers
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All consumers stopped gracefully")
	case <-time.After(30 * time.Second):
		log.Println("Timeout waiting for consumers to stop")
	}

	// Останавливаем HTTP сервер
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Закрываем consumers
	for _, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}

	log.Println("Server exited gracefully")
}
