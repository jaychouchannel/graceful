// Command graceful is a demo binary that showcases the graceful package
// by running a small HTTP server, a background worker, and a fake DB,
// then shutting them all down on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jaychouchannel/graceful"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "graceful:", err)
		os.Exit(1)
	}
}

func run() error {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Fake DB with connection pool semantics.
	db := &fakeDB{name: "primary", open: 10}
	if err := db.connect(); err != nil {
		return err
	}

	// HTTP server with a handler that touches the DB.
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if db.healthy() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Simulate a request that takes some time, so drain is visible.
		delay := time.Duration(rand.Intn(200)) * time.Millisecond
		time.Sleep(delay)
		_, _ = fmt.Fprintf(w, "hello (db=%s, open=%d, delay=%v)\n", db.name, db.openNow(), delay)
	})
	srv := &http.Server{Addr: ":8080", Handler: mux}

	// Background worker that ticks.
	workerCtx, workerCancel := context.WithCancel(context.Background())
	var workerWg sync.WaitGroup
	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				log.Printf("worker: stopping")
				return
			case <-ticker.C:
				log.Printf("worker: tick (db open=%d)", db.openNow())
			}
		}
	}()

	m := graceful.NewManager(graceful.Options{
		ShutdownTimeout: 15 * time.Second,
		OnTaskStart: func(name string) {
			log.Printf("shutdown: starting %s", name)
		},
		OnTaskError: func(name string, err error) {
			log.Printf("shutdown: %s error: %v", name, err)
		},
	})

	m.Register(graceful.Task{
		Name: "http-server",
		Stop: func(ctx context.Context) error {
			log.Printf("http-server: draining in-flight requests (ctx=%v)", ctx.Err())
			if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
		// HTTP server gets the biggest budget because it needs to drain.
		Timeout: 10 * time.Second,
	})
	m.Register(graceful.Task{
		Name: "worker",
		Stop: func(ctx context.Context) error {
			workerCancel()
			workerWg.Wait()
			return nil
		},
		Timeout: 3 * time.Second,
	})
	m.Register(graceful.Task{
		Name: "db",
		Stop: func(ctx context.Context) error {
			return db.close(ctx)
		},
		// DB is last (registered last in shutdown order) and gets the smallest budget.
		Timeout: 2 * time.Second,
	})

	// Start HTTP server in a goroutine so we can block on m.Shutdown below.
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http-server: %v", err)
		}
	}()

	return m.Shutdown()
}

type fakeDB struct {
	name     string
	open     int
	mu       sync.Mutex
	closed   bool
	openedInFlight int
}

func (d *fakeDB) connect() error {
	log.Printf("db[%s]: connecting (%d connections)", d.name, d.open)
	return nil
}

func (d *fakeDB) healthy() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.closed
}

func (d *fakeDB) openNow() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.open + d.openedInFlight
}

func (d *fakeDB) close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	log.Printf("db[%s]: closing (waiting for in-flight queries)", d.name)
	// Simulate waiting for in-flight queries to drain.
	time.Sleep(200 * time.Millisecond)
	log.Printf("db[%s]: closed", d.name)
	return nil
}
