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

## Usage

Register components during startup, then call `Shutdown` on exit:

```go
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 30 * time.Second,
})

m.Register(graceful.Task{
    Name: "http-server",
    Stop: func(ctx context.Context) error {
        return srv.Shutdown(ctx) // http.Server drains in-flight requests
    },
    Timeout: 10 * time.Second,
})
m.Register(graceful.Task{
    Name: "db",
    Stop:  func(ctx context.Context) error { return db.Close() },
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

## Options

| Field | Default | Description |
|---|---|---|
| `ShutdownTimeout` | 30s | Global deadline across all tasks |
| `Signals` | `[SIGINT, SIGTERM]` (Windows: `[SIGINT]`) | Signals that trigger shutdown |
| `OnTaskStart` | nil | Called when a task begins stopping |
| `OnTaskError` | nil | Called when a task's `Stop` returns an error |

## Roadmap

- [ ] Dependency graph (`DependsOn`) with topological ordering for shutdown
- [ ] `context.WithoutCancel`-based drain reporting (in-flight request count)