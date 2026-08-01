# graceful

Declarative graceful shutdown for Go services, with dependency-aware ordering and timeout orchestration.

## Why this instead of writing your own

Every Go service needs to drain in-flight requests and stop workers in a controlled order when a signal arrives, then exit with a deadlock-free timeout. This library encodes that loop so you don't rewrite it:

- **Per-task timeout + global deadline.** Each component gets its own shutdown budget, capped by an overall deadline, so one hung worker can't stall forever.
- **Panic-safe.** A panicking `Stop` is recovered and reported, not allowed to tear down the whole process.
- **Concurrent shutdown.** Independent tasks stop in parallel; total time is bounded by the slowest task, not the sum.
- **Idempotent.** Calling `Shutdown` twice is safe.
- **Zero dependencies.** Uses only the standard library.
- **Cross-platform.** Uses `signal.NotifyContext`. Defaults to `os.Interrupt` on Windows where `SIGTERM` is unavailable.

## Install

```sh
go get github.com/jaychouchannel/graceful
```

## Quick start

Register components during startup, then call `Shutdown` on exit:

```go
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 30 * time.Second,
})

m.Register(graceful.Task{
    Name:    "http-server",
    Stop:    func(ctx context.Context) error { return srv.Shutdown(ctx) },
    Timeout: 10 * time.Second,
})
m.Register(graceful.Task{
    Name:    "db",
    Stop:    func(ctx context.Context) error { return db.Close() },
    Timeout: 5 * time.Second,
})

// Triggered by SIGINT/SIGTERM, or call it directly.
err := m.Shutdown()
```

The default behavior waits for `SIGINT`/`SIGTERM` before draining (so call it in `main` and let the process exit when an external signal arrives). To shut down on your own terms, use the list-based helper:

```go
ctx, cancel := context.WithCancel(context.Background())
go func() {
    // e.g. wait for a custom stop condition
    <-stopCh
    cancel()
}()

err := graceful.Run(ctx, graceful.Options{}, []graceful.Task{serverTask, dbTask})
```

## How it works

```
SIGINT / SIGTERM / ctx cancelled
        │
        ▼
┌─────────────────────────────────────┐
│  NotifyContext fires                │
│  (or Run's ctx is cancelled)        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  For each registered task (concurrent)│
│  ┌─────────────────────────────────┐ │
│  │  OnTaskStart(name)             │ │
│  │  taskCtx, cancel = WithTimeout │ │
│  │    (deadline = ShutdownTimeout) │ │
│  │  Stop(taskCtx)                 │ │
│  │    → recover panic → report    │ │
│  │    → error → OnTaskError       │ │
│  │  OnTaskDone                    │ │
│  └─────────────────────────────────┘ │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  WaitGroup.Wait() or deadline hit   │
│  If deadline exceeded → ErrTimeout  │
│  If any task error → aggregated     │
└──────────────┬──────────────────────┘
               │
               ▼
          return err
```

## API reference

### `NewManager(opts Options) *Manager`

Creates a new shutdown manager. Zero-value `ShutdownTimeout` defaults to 30s; zero-value `Signals` defaults to `[os.Interrupt]` (Windows) or `[os.Interrupt, syscall.SIGTERM]` (Unix).

### `(*Manager).Register(task Task)`

Registers a component to be stopped during shutdown. Safe to call before or after `Shutdown` has been triggered (tasks registered after shutdown has started will still be stopped).

### `(*Manager).Shutdown() error`

Blocks until all tasks have stopped or the global timeout is reached. Safe to call multiple times — subsequent calls are no-ops and return `nil`. Returns `ErrTimeout` wrapped in an error if the global deadline is exceeded, or an aggregated error if any task's `Stop` returned an error.

### `Run(ctx context.Context, opts Options, tasks []Task) error`

Convenience function. Creates a `Manager`, registers all tasks, and calls `Shutdown`. Shutdown is triggered when `ctx` is cancelled or a configured OS signal is received.

### `Task`

| Field    | Type                  | Description                                      |
|----------|-----------------------|--------------------------------------------------|
| `Name`   | `string`              | Identifier used in callbacks and error messages  |
| `Stop`   | `func(ctx) error`     | Drains the component; receives a context with the per-task timeout |
| `Timeout`| `time.Duration`       | Per-task budget; zero means the global deadline  |

### `Options`

| Field         | Default                          | Description                                      |
|---------------|----------------------------------|--------------------------------------------------|
| `ShutdownTimeout` | `30s`                        | Global deadline across all tasks                 |
| `Signals`     | `[SIGINT, SIGTERM]` (Windows: `[SIGINT]`) | Signals that trigger shutdown      |
| `OnTaskStart` | `nil`                            | Called when a task begins stopping               |
| `OnTaskError` | `nil`                            | Called when a task's `Stop` returns an error     |

### `ErrTimeout`

Returned (wrapped) when the global `ShutdownTimeout` is exceeded before all tasks finished.

## Callbacks

Use `OnTaskStart` and `OnTaskError` to hook into the shutdown lifecycle for logging or metrics:

```go
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 30 * time.Second,
    OnTaskStart: func(name string) {
        log.Printf("stopping %s...", name)
    },
    OnTaskError: func(name string, err error) {
        log.Printf("task %s error: %v", name, err)
    },
})
```

## Testing

`Manager` exposes an internal `testTrigger` channel for deterministic tests. Use `NewManager` with `testTrigger` set to fire shutdown without waiting for a real signal:

```go
trigger := make(chan struct{})
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 5 * time.Second,
})
// inject trigger via internal field (or use Run with a pre-cancelled ctx)
```

See `graceful_test.go` for full examples.

## CLI demo

A demo binary is included at `cmd/graceful`. It runs a small HTTP server, a background worker, and a fake DB, then shuts them all down on `SIGINT`/`SIGTERM`:

```sh
go run ./cmd/graceful
# or build and run
go build -o graceful ./cmd/graceful
./graceful
```

## Docker

Build and run the demo container:

```sh
docker build -t graceful .
docker run -p 8080:8080 graceful
```

The `Dockerfile` uses a single-stage Alpine build with `CGO_ENABLED=0` for a static binary.

## Roadmap

- [ ] Dependency graph (`DependsOn`) with topological ordering for shutdown
- [ ] `context.WithoutCancel`-based drain reporting (in-flight request count)
