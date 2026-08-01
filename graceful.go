package graceful

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

// ErrTimeout is returned when the global shutdown deadline is exceeded.
var ErrTimeout = errors.New("graceful shutdown timed out")

// ResultStatus describes the outcome of a single task's shutdown.
type ResultStatus int

const (
	// ResultOK means the task's Stop returned nil.
	ResultOK ResultStatus = iota
	// ResultError means the task's Stop returned a non-nil error.
	ResultError
	// ResultTimeout means the task's Stop did not finish before its
	// per-task deadline (the global deadline may still be active).
	ResultTimeout
	// ResultPanic means the task's Stop panicked; the panic was
	// recovered and reported.
	ResultPanic
)

// Result captures the outcome of a single task's shutdown.
type Result struct {
	Name   string
	Status ResultStatus
	Err    error
}

// Task represents a single component to shut down.
type Task struct {
	// Name is a unique identifier for the task, used in callbacks,
	// error messages, and dependency declarations.
	Name string

	// Stop drains the component. It receives a context whose deadline is
	// the smaller of the global ShutdownTimeout and the task's Timeout.
	Stop func(ctx context.Context) error

	// Timeout is the per-task budget. Zero means the task inherits the
	// global ShutdownTimeout deadline.
	Timeout time.Duration

	// DependsOn lists the Names of tasks that must finish stopping before
	// this task begins. Tasks with unmet dependencies are deferred to a
	// later wave; independent tasks with no dependencies stop in parallel.
	// Unknown names are ignored.
	DependsOn []string
}

// Manager orchestrates graceful shutdown of registered tasks.
type Manager struct {
	opts    Options
	tasks   []Task
	mu      sync.Mutex
	stopped bool

	// ctx, if set via Run, also triggers shutdown when cancelled.
	ctx context.Context

	// stopCh is closed by Stop() to trigger shutdown programmatically.
	stopCh chan struct{}

	// stopOnce ensures Stop() is idempotent.
	stopOnce sync.Once

	// testTrigger, when non-nil, replaces the signal-based trigger for
	// deterministic testing. Internal only.
	testTrigger <-chan struct{}

	// results holds per-task outcomes once shutdown has run.
	results []Result
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
	return &Manager{
		opts:   opts,
		stopCh: make(chan struct{}),
	}
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

	triggerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wire up signal-based trigger.
	if m.testTrigger != nil {
		go func() {
			<-m.testTrigger
			cancel()
		}()
	} else if m.ctx != nil {
		go func() {
			select {
			case <-triggerCtx.Done():
			case <-m.ctx.Done():
				cancel()
			}
		}()
	} else {
		sigCtx, stop := signal.NotifyContext(context.Background(), m.opts.Signals...)
		defer stop()
		go func() {
			select {
			case <-triggerCtx.Done():
			case <-sigCtx.Done():
				cancel()
			}
		}()
	}

	// Wire up programmatic Stop() trigger.
	go func() {
		<-m.stopCh
		cancel()
	}()

	<-triggerCtx.Done()
	return m.shutdownWithCtx(tasks)
}

// Stop triggers shutdown programmatically, without waiting for an OS signal.
// Safe to call from any goroutine. Multiple calls are no-ops.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

// Results returns per-task outcomes after Shutdown has completed.
// Returns nil if Shutdown has not yet been called.
func (m *Manager) Results() []Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.results == nil {
		return nil
	}
	res := make([]Result, len(m.results))
	copy(res, m.results)
	return res
}

// shutdownWithCtx performs the actual shutdown once a trigger has fired.
// It respects dependency ordering via wave-based scheduling and collects
// per-task Result values.
func (m *Manager) shutdownWithCtx(tasks []Task) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), m.opts.ShutdownTimeout)
	defer cancel()

	// Build dependency graph. remaining counts unmet dependencies per task,
	// dependents maps a task to the names that depend on it.
	taskByName := make(map[string]Task, len(tasks))
	for _, t := range tasks {
		taskByName[t.Name] = t
	}

	// Detect dependency cycles; a cycle means no task in it can ever be
	// scheduled, so fail fast rather than silently skipping work.
	if cyc := cycle(taskByName); cyc != "" {
		return fmt.Errorf("graceful: dependency cycle detected: %s", cyc)
	}

	remaining := make(map[string]int, len(tasks))
	dependents := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		remaining[t.Name] = len(t.DependsOn)
		for _, dep := range t.DependsOn {
			if _, ok := taskByName[dep]; ok {
				dependents[dep] = append(dependents[dep], t.Name)
			}
		}
	}

	var mu sync.Mutex
	var results []Result
	var errs []error

	// work holds tasks whose dependencies are all satisfied. Independent
	// tasks at the same depth run concurrently; dependents wait for their
	// dependencies' entire batch to finish before starting.
	var work []Task
	for _, t := range tasks {
		if remaining[t.Name] == 0 {
			work = append(work, t)
		}
	}

	for len(work) > 0 {
		batch := work
		work = nil
		var batchWg sync.WaitGroup

		for _, t := range batch {
			batchWg.Add(1)
			go func(t Task) {
				defer batchWg.Done()
				m.runTask(shutdownCtx, t, &mu, &results, &errs)
				// Release dependents whose dependencies are now met.
				mu.Lock()
				for _, name := range dependents[t.Name] {
					remaining[name]--
					if remaining[name] == 0 {
						work = append(work, taskByName[name])
					}
				}
				mu.Unlock()
			}(t)
		}
		batchWg.Wait()
	}

	m.mu.Lock()
	m.results = results
	m.mu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("graceful shutdown completed with errors: %w", errs[0])
	}
	if shutdownCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("%w: not all tasks finished in time", ErrTimeout)
	}
	return nil
}

// cycle returns a human-readable chain of a dependency cycle in tasks, or
// the empty string if the dependency graph is acyclic. Unknown dependency
// names (not present among the registered tasks) are ignored.
func cycle(byName map[string]Task) string {
	state := make(map[string]int, len(byName)) // 0=unvisited, 1=visiting, 2=done
	var visit func(name string, chain []string) string
	visit = func(name string, chain []string) string {
		switch state[name] {
		case 2:
			return ""
		case 1:
			// Back edge: name repeats in the current DFS chain.
			for i, c := range chain {
				if c == name {
					return strings.Join(append(append([]string{}, chain[i:]...), name), " -> ")
				}
			}
			return ""
		}
		state[name] = 1
		nextChain := append(append([]string{}, chain...), name)
		for _, dep := range byName[name].DependsOn {
			if _, ok := byName[dep]; !ok {
				continue // unknown dependency, ignore
			}
			if c := visit(dep, nextChain); c != "" {
				return c
			}
		}
		state[name] = 2
		return ""
	}

	for name := range byName {
		if c := visit(name, nil); c != "" {
			return c
		}
	}
	return ""
}

// runTask executes a single task's Stop, records the result, and calls
// lifecycle callbacks. It recovers from panics and maps them to ResultPanic.
func (m *Manager) runTask(shutdownCtx context.Context, t Task, mu *sync.Mutex, results *[]Result, errs *[]error) {
	var status ResultStatus
	var err error

	defer func() {
		if r := recover(); r != nil {
			mu.Lock()
			*results = append(*results, Result{Name: t.Name, Status: ResultPanic, Err: fmt.Errorf("panic: %v", r)})
			*errs = append(*errs, fmt.Errorf("task %q panicked: %v", t.Name, r))
			mu.Unlock()
			if m.opts.OnTaskError != nil {
				m.opts.OnTaskError(t.Name, fmt.Errorf("panic: %v", r))
			}
		}
	}()

	if m.opts.OnTaskStart != nil {
		m.opts.OnTaskStart(t.Name)
	}

	// Zero Timeout means the task inherits only the global shutdown deadline.
	taskCtx := shutdownCtx
	if t.Timeout > 0 {
		var taskCancel context.CancelFunc
		taskCtx, taskCancel = context.WithTimeout(shutdownCtx, t.Timeout)
		defer taskCancel()
	}

	if err = t.Stop(taskCtx); err != nil {
		if errors.Is(taskCtx.Err(), context.DeadlineExceeded) {
			status = ResultTimeout
		} else {
			status = ResultError
		}
		mu.Lock()
		*results = append(*results, Result{Name: t.Name, Status: status, Err: err})
		*errs = append(*errs, fmt.Errorf("task %q: %w", t.Name, err))
		mu.Unlock()
		if m.opts.OnTaskError != nil {
			m.opts.OnTaskError(t.Name, err)
		}
		return
	}

	mu.Lock()
	*results = append(*results, Result{Name: t.Name, Status: ResultOK})
	mu.Unlock()
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

// defaultSignals is defined in grace_signals_unix.go / grace_signals_windows.go
// to handle the absence of syscall.SIGTERM on Windows.
