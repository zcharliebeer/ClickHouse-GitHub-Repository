package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	batchSize    = 10000
	batchTimeout = 2 * time.Second
	chanCapacity = 50000
)

type Event struct {
	Time    time.Time
	Payload string
}

type Batcher struct {
	eventChan chan Event
	batchChan chan []Event
	buffer    []Event
}

func NewBatcher(eventChan chan Event, batchChan chan []Event) *Batcher {
	return &Batcher{
		eventChan: eventChan,
		batchChan: batchChan,
		buffer:    make([]Event, 0, batchSize),
	}
}

func (b *Batcher) Start(ctx context.Context) {
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-b.eventChan:
			if !ok {
				b.flush()
				return
			}
			b.buffer = append(b.buffer, event)
			if len(b.buffer) >= batchSize {
				b.flush()
				ticker.Reset(batchTimeout)
			}
		case <-ticker.C:
			if len(b.buffer) > 0 {
				b.flush()
			}
		case <-ctx.Done():
			b.flush()
			return
		}
	}
}

func (b *Batcher) flush() {
	if len(b.buffer) == 0 {
		return
	}
	batch := make([]Event, len(b.buffer))
	copy(batch, b.buffer)
	b.buffer = b.buffer[:0]

	b.batchChan <- batch
}

func worker(id int, conn clickhouse.Conn, batchChan <-chan []Event, wg *sync.WaitGroup) {
	defer wg.Done()
	for batch := range batchChan {
		err := writeBatch(conn, batch)
		if err != nil {
			log.Printf("Worker %d: error writing batch: %v", id, err)
		}
	}
}

func writeBatch(conn clickhouse.Conn, batch []Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	clickhouseBatch, err := conn.PrepareBatch(ctx, "INSERT INTO github_events (event_time, payload)")
	if err != nil {
		return fmt.Errorf("prepare batch failed: %w", err)
	}

	for _, event := range batch {
		err := clickhouseBatch.Append(event.Time, event.Payload)
		if err != nil {
			return fmt.Errorf("append to batch failed: %w", err)
		}
	}

	err = clickhouseBatch.Send()
	if err != nil {
		return fmt.Errorf("send batch failed: %w", err)
	}

	return nil
}

func main() {
	log.Println("Starting ClickHouse GitHub Ingestion Service...")

	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	dbName := os.Getenv("CLICKHOUSE_DB")
	if dbName == "" {
		dbName = "default"
	}
	user := os.Getenv("CLICKHOUSE_USER")
	if user == "" {
		user = "default"
	}
	password := os.Getenv("CLICKHOUSE_PASSWORD")

	numCPU := runtime.NumCPU()
	if numCPU < 2 {
		numCPU = 2
	}

	opts := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: dbName,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"async_insert":          1,
			"wait_for_async_insert": 1,
		},
		MaxOpenConns: numCPU,
		MaxIdleConns: numCPU,
		DialTimeout:  5 * time.Second,
	}

	var conn clickhouse.Conn
	var err error
	for i := 0; i < 10; i++ { 
		conn, err = clickhouse.Open(opts)
		if err == nil {
			err = conn.Ping(context.Background())
			if err == nil {
				break
			}
		}
		log.Printf("Failed to connect to ClickHouse, retrying in 2s... (%d/10): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	// Create table if not exists
	query := `
	CREATE TABLE IF NOT EXISTS github_events (
		event_time DateTime64(3) DEFAULT now(),
		payload String
	) ENGINE = MergeTree()
	ORDER BY event_time;
	`
	if err := conn.Exec(context.Background(), query); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	eventChan := make(chan Event, chanCapacity)
	batchChan := make(chan []Event, numCPU)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start batcher
	batcher := NewBatcher(eventChan, batchChan)
	batcherDone := make(chan struct{})
	go func() {
		batcher.Start(ctx)
		close(batcherDone)
	}()

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go worker(i, conn, batchChan, &wg)
	}

	// HTTP Handler
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if r.Method == http.MethodGet {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
				return
			}
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if len(body) == 0 {
			http.Error(w, "Empty body", http.StatusBadRequest)
			return
		}

		event := Event{
			Time:    time.Now(),
			Payload: string(body),
		}

		select {
		case eventChan <- event:
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(`{"status":"accepted"}`))
		default:
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"buffer full, try again later"}`))
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr: ":" + port,
	}

	go func() {
		log.Printf("HTTP server listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Graceful shutdown setup
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Println("Shutting down gracefully...")

	// 1. Shutdown HTTP server
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server Shutdown error: %v", err)
	}

	// 2. Close eventChan to stop batcher
	close(eventChan)

	// Wait for batcher to finish flushing
	<-batcherDone

	// 3. Close batchChan to stop workers
	close(batchChan)

	// 4. Wait for workers to finish writing
	wg.Wait()

	log.Println("Shutdown complete.")
}