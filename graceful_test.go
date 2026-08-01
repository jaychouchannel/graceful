package graceful

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// triggerShutdown starts Shutdown in the background and fires it via Stop().
// Returns when Shutdown has returned. The caller should wait on stopCtx.
func startShutdown(t *testing.T, m *Manager) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- m.Shutdown()
	}()
	// Give Shutdown a moment to set up its signal listener before Stop() fires.
	time.Sleep(10 * time.Millisecond)
	return done
}

func TestShutdownAllTasks(t *testing.T) {
	var mu sync.Mutex
	var stopped []string
	tasks := []Task{
		{
			Name: "task-a",
			Stop: func(ctx context.Context) error {
				mu.Lock()
				stopped = append(stopped, "task-a")
				mu.Unlock()
				return nil
			},
			Timeout: 5 * time.Second,
		},
		{
			Name: "task-b",
			Stop: func(ctx context.Context) error {
				mu.Lock()
				stopped = append(stopped, "task-b")
				mu.Unlock()
				return nil
			},
			Timeout: 5 * time.Second,
		},
	}

	m := NewManager(Options{})
	for _, task := range tasks {
		m.Register(task)
	}

	done := startShutdown(t, m)
	m.Stop()
	if err := <-done; err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(stopped) != 2 {
		t.Fatalf("expected 2 tasks stopped, got %d: %v", len(stopped), stopped)
	}
}

func TestShutdownTimeout(t *testing.T) {
	slowTask := Task{
		Name: "slow",
		Stop: func(ctx context.Context) error {
			select {
			case <-time.After(10 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		Timeout: 100 * time.Millisecond,
	}

	m := NewManager(Options{ShutdownTimeout: 200 * time.Millisecond})
	m.Register(slowTask)

	done := startShutdown(t, m)
	m.Stop()
	err := <-done

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// Either the global deadline was exceeded or the per-task deadline expired.
	if !strings.Contains(err.Error(), "timed out") && !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("expected timeout-related error, got %v", err)
	}
}

func TestShutdownTaskPanic(t *testing.T) {
	panicTask := Task{
		Name: "panic-task",
		Stop: func(ctx context.Context) error {
			panic("oops")
		},
		Timeout: 5 * time.Second,
	}

	m := NewManager(Options{})
	m.Register(panicTask)

	done := startShutdown(t, m)
	m.Stop()
	<-done

	// Panic should be recovered and reported as ResultPanic, not crash the process.
	var found bool
	for _, r := range m.Results() {
		if r.Name == "panic-task" && r.Status == ResultPanic {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected ResultPanic for panic-task, got %v", m.Results())
	}
}

func TestShutdownIdempotent(t *testing.T) {
	var mu sync.Mutex
	count := 0
	m := NewManager(Options{})
	m.Register(Task{
		Name: "once",
		Stop: func(ctx context.Context) error {
			mu.Lock()
			count++
			mu.Unlock()
			return nil
		},
		Timeout: 5 * time.Second,
	})

	done := startShutdown(t, m)
	m.Stop()
	if err := <-done; err != nil {
		t.Fatalf("first Shutdown returned error: %v", err)
	}
	if err := m.Shutdown(); err != nil { // second call should be a no-op
		t.Fatalf("second Shutdown returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("expected task to stop exactly once, got %d", count)
	}
}

func TestShutdownZeroTasks(t *testing.T) {
	m := NewManager(Options{})
	done := startShutdown(t, m)
	m.Stop()
	if err := <-done; err != nil {
		t.Fatalf("expected no error with zero tasks, got %v", err)
	}
}

func TestStopIdempotent(t *testing.T) {
	var mu sync.Mutex
	count := 0
	m := NewManager(Options{})
	m.Register(Task{
		Name: "stop-once",
		Stop: func(ctx context.Context) error {
			mu.Lock()
			count++
			mu.Unlock()
			return nil
		},
		Timeout: 5 * time.Second,
	})

	done := startShutdown(t, m)
	m.Stop()
	m.Stop() // multiple Stop() calls must be no-ops
	if err := <-done; err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("expected task to run exactly once, got %d", count)
	}
}

func TestRunWithTasks(t *testing.T) {
	var mu sync.Mutex
	var stopped []string
	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan error, 1)
	go func() {
		runDone <- Run(ctx, Options{}, []Task{
			{
				Name: "r-a",
				Stop: func(ctx context.Context) error {
					mu.Lock()
					stopped = append(stopped, "r-a")
					mu.Unlock()
					return nil
				},
				Timeout: 5 * time.Second,
			},
		})
	}()

	// Give Run a moment to set up before cancelling.
	time.Sleep(20 * time.Millisecond)
	cancel()

	if err := <-runDone; err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(stopped) != 1 {
		t.Fatalf("expected 1 task stopped, got %d", len(stopped))
	}
}

func TestOnTaskStartAndOnTaskError(t *testing.T) {
	var mu sync.Mutex
	var started, errored []string
	m := NewManager(Options{
		OnTaskStart: func(name string) {
			mu.Lock()
			started = append(started, name)
			mu.Unlock()
		},
		OnTaskError: func(name string, err error) {
			mu.Lock()
			errored = append(errored, name)
			mu.Unlock()
		},
	})

	m.Register(Task{
		Name:    "good",
		Stop:    func(ctx context.Context) error { return nil },
		Timeout: 5 * time.Second,
	})
	m.Register(Task{
		Name:    "bad",
		Stop:    func(ctx context.Context) error { return errors.New("boom") },
		Timeout: 5 * time.Second,
	})

	done := startShutdown(t, m)
	m.Stop()
	err := <-done
	if err == nil {
		t.Fatal("expected error from bad task")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(started) != 2 {
		t.Fatalf("expected 2 tasks started, got %d", len(started))
	}
	if len(errored) != 1 || errored[0] != "bad" {
		t.Fatalf("expected [bad] in errored, got %v", errored)
	}
}

// --- New feature tests ---

func TestDependsOnOrdering(t *testing.T) {
	var mu sync.Mutex
	var order []string
	m := NewManager(Options{})

	m.Register(Task{
		Name: "combined",
		Stop: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "combined")
			mu.Unlock()
			return nil
		},
		DependsOn: []string{"db", "http"},
	})
	m.Register(Task{
		Name: "db",
		Stop: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "db")
			mu.Unlock()
			return nil
		},
	})
	m.Register(Task{
		Name: "http",
		Stop: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "http")
			mu.Unlock()
			return nil
		},
		DependsOn: []string{"db"},
	})

	done := startShutdown(t, m)
	m.Stop()
	if err := <-done; err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// db must stop before http, and http before combined.
	if len(order) != 3 {
		t.Fatalf("expected 3 stops in order, got %v", order)
	}
	if order[0] != "db" || order[1] != "http" || order[2] != "combined" {
		t.Fatalf("expected [db http combined] order, got %v", order)
	}
}

func TestDependsOnCycle(t *testing.T) {
	m := NewManager(Options{})
	m.Register(Task{
		Name:       "a",
		Stop:       func(ctx context.Context) error { return nil },
		DependsOn:  []string{"b"},
	})
	m.Register(Task{
		Name:       "b",
		Stop:       func(ctx context.Context) error { return nil },
		DependsOn:  []string{"a"},
	})

	done := startShutdown(t, m)
	m.Stop()
	err := <-done
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected a cycle error, got %v", err)
	}
}

func TestResultsStatuses(t *testing.T) {
	m := NewManager(Options{})
	m.Register(Task{Name: "ok", Stop: func(ctx context.Context) error { return nil }})
	m.Register(Task{Name: "bad", Stop: func(ctx context.Context) error { return errors.New("x") }})
	m.Register(Task{Name: "panic", Stop: func(ctx context.Context) error { panic("boom") }})

	done := startShutdown(t, m)
	m.Stop()
	if err := <-done; err == nil {
		t.Fatal("expected Shutdown to report errors")
	}

	statuses := map[string]ResultStatus{}
	for _, r := range m.Results() {
		statuses[r.Name] = r.Status
	}
	if statuses["ok"] != ResultOK {
		t.Errorf("expected ok=ResultOK, got %v", statuses["ok"])
	}
	if statuses["bad"] != ResultError {
		t.Errorf("expected bad=ResultError, got %v", statuses["bad"])
	}
	if statuses["panic"] != ResultPanic {
		t.Errorf("expected panic=ResultPanic, got %v", statuses["panic"])
	}
}

func TestResultsNilBeforeShutdown(t *testing.T) {
	m := NewManager(Options{})
	if r := m.Results(); r != nil {
		t.Fatalf("expected nil results before Shutdown, got %v", r)
	}
}
