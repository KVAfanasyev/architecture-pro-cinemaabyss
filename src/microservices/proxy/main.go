package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// Config holds service configuration
type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

// HealthResponse represents health check response
type HealthResponse struct {
	Status    bool                   `json:"status"`
	Services  map[string]bool        `json:"services"`
	Migration map[string]interface{} `json:"migration"`
}

var (
	config     Config
	httpClient = &http.Client{
		Timeout: 30 * time.Second,
	}
)

func main() {
	loadConfig()

	r := mux.NewRouter()

	// Health check endpoint
	r.HandleFunc("/health", healthHandler).Methods("GET")

	// API routes with migration logic
	r.HandleFunc("/api/movies", moviesHandler)
	r.HandleFunc("/api/movies/{id}", movieByIDHandler)
	r.HandleFunc("/api/movies/health", moviesHealthHandler)

	// Events service routes (direct proxy)
	r.HandleFunc("/api/events", eventsHandler)
	r.HandleFunc("/api/events/health", eventsHealthHandler)

	// Monolith routes (direct proxy for non-migrated services)
	r.HandleFunc("/api/users", usersHandler)
	r.HandleFunc("/api/users/{id}", userByIDHandler)
	r.HandleFunc("/api/payments", paymentsHandler)
	r.HandleFunc("/api/subscriptions", subscriptionsHandler)
	r.HandleFunc("/api/health", monolithHealthHandler)

	// Catch-all for other monolith routes
	r.PathPrefix("/").HandlerFunc(monolithHandler)

	log.Printf("Starting proxy service on port %s", config.Port)
	log.Printf("Movies migration: %d%% traffic to microservice", config.MoviesMigrationPercent)
	log.Printf("Gradual migration enabled: %v", config.GradualMigration)

	log.Fatal(http.ListenAndServe(":"+config.Port, r))
}

func loadConfig() {
	config = Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            getEnv("MONOLITH_URL", "http://localhost:8080"),
		MoviesServiceURL:       getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"),
		EventsServiceURL:       getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"),
		GradualMigration:       getEnvAsBool("GRADUAL_MIGRATION", true),
		MoviesMigrationPercent: getEnvAsInt("MOVIES_MIGRATION_PERCENT", 50),
	}

	// Validate migration percentage
	if config.MoviesMigrationPercent < 0 || config.MoviesMigrationPercent > 100 {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT: %d, defaulting to 50", config.MoviesMigrationPercent)
		config.MoviesMigrationPercent = 50
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	health := HealthResponse{
		Status:   true,
		Services: make(map[string]bool),
		Migration: map[string]interface{}{
			"gradual_migration": config.GradualMigration,
			"movies_percent":    config.MoviesMigrationPercent,
		},
	}

	// Check monolith health
	monolithHealth := checkServiceHealth(config.MonolithURL + "/health")
	health.Services["monolith"] = monolithHealth

	// Check movies service health
	moviesHealth := checkServiceHealth(config.MoviesServiceURL + "/api/movies/health")
	health.Services["movies_service"] = moviesHealth

	// Check events service health
	eventsHealth := checkServiceHealth(config.EventsServiceURL + "/health")
	health.Services["events_service"] = eventsHealth

	// Overall status is false if any critical service is down
	if !monolithHealth || !moviesHealth {
		health.Status = false
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

func checkServiceHealth(url string) bool {
	resp, err := httpClient.Get(url)
	if err != nil {
		log.Printf("Health check failed for %s: %v", url, err)
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// Migration decision logic
func shouldRouteToMicroservice() bool {
	if !config.GradualMigration {
		return true // Full migration if gradual is disabled
	}

	// Generate random number between 1-100
	randomValue := rand.Intn(100) + 1
	return randomValue <= config.MoviesMigrationPercent
}

// Movies handlers with migration logic
func moviesHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Movies request: %s %s", r.Method, r.URL.String())

	if shouldRouteToMicroservice() {
		log.Printf("Routing to movies microservice")
		proxyRequest(w, r, config.MoviesServiceURL, "/api/movies")
	} else {
		log.Printf("Routing to monolith")
		proxyRequest(w, r, config.MonolithURL, "/api/movies")
	}
}

func movieByIDHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	movieID := vars["id"]
	log.Printf("Movie by ID request: %s %s (ID: %s)", r.Method, r.URL.String(), movieID)

	if shouldRouteToMicroservice() {
		log.Printf("Routing to movies microservice for movie ID: %s", movieID)
		if r.Method == "GET" {
			proxyRequest(w, r, config.MoviesServiceURL, "/api/movies?id="+movieID)
		} else {
			proxyRequest(w, r, config.MoviesServiceURL, "/api/movies")
		}
	} else {
		log.Printf("Routing to monolith for movie ID: %s", movieID)
		proxyRequest(w, r, config.MonolithURL, "/api/movies?id="+movieID)
	}
}

func moviesHealthHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.MoviesServiceURL, "/api/movies/health")
}

// Direct proxy handlers for other services
func eventsHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.EventsServiceURL, "/api/events")
}

func eventsHealthHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.EventsServiceURL, "/health")
}

func usersHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.MonolithURL, "/api/users")
}

func userByIDHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	proxyRequest(w, r, config.MonolithURL, "/api/users?id="+userID)
}

func paymentsHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.MonolithURL, "/api/payments")
}

func subscriptionsHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.MonolithURL, "/api/subscriptions")
}

func monolithHealthHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.MonolithURL, "/health")
}

func monolithHandler(w http.ResponseWriter, r *http.Request) {
	proxyRequest(w, r, config.MonolithURL, r.URL.String())
}

// Generic proxy function
func proxyRequest(w http.ResponseWriter, r *http.Request, targetBaseURL, targetPath string) {
	target, err := url.Parse(targetBaseURL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid target URL: %v", err), http.StatusInternalServerError)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Modify the request
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)

		// Update the path
		if targetPath != "" {
			// Handle query parameters properly
			if strings.Contains(targetPath, "?") {
				// Path already contains query params
				req.URL.Path = strings.Split(targetPath, "?")[0]
				req.URL.RawQuery = strings.Split(targetPath, "?")[1]
			} else {
				req.URL.Path = targetPath
			}
		}

		// Preserve original query parameters for non-prebuilt queries
		if r.URL.RawQuery != "" && !strings.Contains(targetPath, "?") {
			if req.URL.RawQuery != "" {
				req.URL.RawQuery += "&" + r.URL.RawQuery
			} else {
				req.URL.RawQuery = r.URL.RawQuery
			}
		}

		// Set headers
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Proxy-Service", "cinemaabyss-proxy")
		req.Header.Set("X-Migration-Percent", strconv.Itoa(config.MoviesMigrationPercent))

		log.Printf("Proxying request to: %s%s", targetBaseURL, req.URL.String())
	}

	// Error handling
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error: %v", err)
		http.Error(w, fmt.Sprintf("Service unavailable: %v", err), http.StatusBadGateway)
	}

	// Modify response to add proxy headers
	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Set("X-Proxy-Service", "cinemaabyss-proxy")
		resp.Header.Set("X-Upstream-Service", target.Hostname())
		return nil
	}

	// Serve the request
	proxy.ServeHTTP(w, r)
}

// Helper function for simple HTTP proxy without complex path manipulation
func simpleProxy(w http.ResponseWriter, r *http.Request, targetURL string) {
	target, err := url.Parse(targetURL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid target URL: %v", err), http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ServeHTTP(w, r)
}

// Initialize random seed
func init() {
	rand.Seed(time.Now().UnixNano())
}
