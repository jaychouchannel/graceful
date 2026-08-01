package graceful

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

var (
	// ErrTimeout is returned when the global shutdown deadline is exceeded.
	ErrTimeout = errors.New("graceful shutdown timed out")
)

// Task represents a single component to shut down.
type Task struct {
	Name    string
	Stop    func(ctx context.Context) error
	Timeout time.Duration
}

// Manager orchestrates graceful shutdown of registered tasks.
type Manager struct {
	opts    Options
	tasks   []Task
	mu      sync.Mutex
	stopped bool

	// ctx, if set via Run, also triggers shutdown when cancelled.
	ctx context.Context

	// testTrigger, when non-nil, replaces the signal-based trigger for
	// deterministic testing. Internal only.
	testTrigger <-chan struct{}
}

// Options configures the Manager.
type Options struct {
	// ShutdownTimeout is the global deadline for all tasks combined.
	// Zero means 30 seconds.
	ShutdownTimeout time.Duration

	// Signals are the OS signals that trigger shutdown.
	// Zero means SIGINT and SIGTERM.
	Signals []os.Signal

	// OnTaskStart is called when a task begins stopping.
	OnTaskStart func(name string)

	// OnTaskError is called when a task's Stop returns an error.
	OnTaskError func(name string, err error)
}

// NewManager creates a new Manager with the given options.
func NewManager(opts Options) *Manager {
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = 30 * time.Second
	}
	if len(opts.Signals) == 0 {
		opts.Signals = defaultSignals()
	}
	return &Manager{opts: opts}
}

// Register adds a task to the shutdown sequence.
func (m *Manager) Register(task Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = append(m.tasks, task)
}

// Shutdown triggers graceful shutdown of all registered tasks.
// It blocks until all tasks have stopped or the global timeout is reached.
// It is safe to call Shutdown multiple times; subsequent calls are no-ops.
func (m *Manager) Shutdown() error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.stopped = true
	tasks := make([]Task, len(m.tasks))
	copy(tasks, m.tasks)
	m.mu.Unlock()

	ctx, stop := signal.NotifyContext(context.Background(), m.opts.Signals...)
	defer stop()

	if m.testTrigger != nil {
		<-m.testTrigger
	} else if m.ctx != nil {
		select {
		case <-ctx.Done():
		case <-m.ctx.Done():
		}
	} else {
		// Wait for signal or context cancellation.
		<-ctx.Done()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), m.opts.ShutdownTimeout)
	defer cancel()

	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup

	for _, task := range tasks {
		wg.Add(1)
		go func(t Task) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("task %q panicked: %v", t.Name, r))
					mu.Unlock()
					if m.opts.OnTaskError != nil {
						m.opts.OnTaskError(t.Name, fmt.Errorf("panic: %v", r))
					}
				}
			}()

			if m.opts.OnTaskStart != nil {
				m.opts.OnTaskStart(t.Name)
			}

			taskCtx, taskCancel := context.WithTimeout(shutdownCtx, t.Timeout)
			defer taskCancel()

			if err := t.Stop(taskCtx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("task %q: %w", t.Name, err))
				mu.Unlock()
				if m.opts.OnTaskError != nil {
					m.opts.OnTaskError(t.Name, err)
				}
			}
		}(task)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-shutdownCtx.Done():
	}

	if len(errs) > 0 {
		return fmt.Errorf("graceful shutdown completed with errors: %w", errs[0])
	}
	if shutdownCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("%w: not all tasks finished in time", ErrTimeout)
	}
	return nil
}

// Run is a convenience function that creates a Manager, registers tasks,
// and runs Shutdown. Shutdown is triggered when ctx is cancelled or a
// configured OS signal is received.
func Run(ctx context.Context, opts Options, tasks []Task) error {
	m := NewManager(opts)
	m.ctx = ctx
	for _, t := range tasks {
		m.Register(t)
	}
	return m.Shutdown()
}

// defaultSignals returns the OS signals that trigger shutdown.
// syscall.SIGTERM is not available on Windows, so we use a build-tag
// approach: on Windows we only listen for os.Interrupt.
func defaultSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
