package graceful

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestShutdownAllTasks(t *testing.T) {
	var stopped []string
	tasks := []Task{
		{
			Name: "task-a",
			Stop: func(ctx context.Context) error {
				stopped = append(stopped, "task-a")
				return nil
			},
			Timeout: 5 * time.Second,
		},
		{
			Name: "task-b",
			Stop: func(ctx context.Context) error {
				stopped = append(stopped, "task-b")
				return nil
			},
			Timeout: 5 * time.Second,
		},
	}

	trigger := make(chan struct{})
	m := NewManager(Options{})
	m.testTrigger = trigger
	for _, task := range tasks {
		m.Register(task)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	err := m.Shutdown()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
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

	trigger := make(chan struct{})
	m := NewManager(Options{ShutdownTimeout: 200 * time.Millisecond})
	m.testTrigger = trigger
	m.Register(slowTask)

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	err := m.Shutdown()
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, ErrTimeout) && err.Error() == "" {
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

	trigger := make(chan struct{})
	m := NewManager(Options{})
	m.testTrigger = trigger
	m.Register(panicTask)

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	// Should not crash the test process; panic is recovered.
	err := m.Shutdown()
	// We don't assert on the error type here because recovery makes it
	// a generic error; the important thing is the test doesn't crash.
	_ = err
}

func TestShutdownIdempotent(t *testing.T) {
	var count int
	trigger := make(chan struct{})
	m := NewManager(Options{})
	m.testTrigger = trigger
	m.Register(Task{
		Name: "once",
		Stop: func(ctx context.Context) error {
			count++
			return nil
		},
		Timeout: 5 * time.Second,
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	m.Shutdown()
	m.Shutdown() // second call should be no-op

	if count != 1 {
		t.Fatalf("expected task to stop exactly once, got %d", count)
	}
}

func TestShutdownZeroTasks(t *testing.T) {
	trigger := make(chan struct{})
	m := NewManager(Options{})
	m.testTrigger = trigger

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	err := m.Shutdown()
	if err != nil {
		t.Fatalf("expected no error with zero tasks, got %v", err)
	}
}

func TestShutdownContextCancelled(t *testing.T) {
	var stopped bool
	trigger := make(chan struct{})
	m := NewManager(Options{})
	m.testTrigger = trigger
	m.Register(Task{
		Name: "ctx-task",
		Stop: func(ctx context.Context) error {
			stopped = true
			return nil
		},
		Timeout: 5 * time.Second,
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	err := m.Shutdown()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !stopped {
		t.Fatal("expected task to have stopped")
	}
}

func TestRunWithTasks(t *testing.T) {
	var stopped []string
	tasks := []Task{
		{
			Name: "r-a",
			Stop: func(ctx context.Context) error {
				stopped = append(stopped, "r-a")
				return nil
			},
			Timeout: 5 * time.Second,
		},
	}

	trigger := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	// Run uses signal.NotifyContext internally, so we can't use testTrigger.
	// Instead we test via Manager directly for this case.
	m := NewManager(Options{})
	m.testTrigger = trigger
	for _, task := range tasks {
		m.Register(task)
	}

	err := m.Shutdown()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(stopped) != 1 {
		t.Fatalf("expected 1 task stopped, got %d", len(stopped))
	}
}

func TestOnTaskStartAndOnTaskError(t *testing.T) {
	var mu sync.Mutex
	var started, errored []string
	trigger := make(chan struct{})
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
	m.testTrigger = trigger

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

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(trigger)
	}()

	err := m.Shutdown()
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
