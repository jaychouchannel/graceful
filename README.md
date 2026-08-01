# graceful

<p align="center">
  <b>Declarative graceful shutdown for Go services</b><br>
  面向 Go 服務的宣告式優雅關閉庫<br>
  面向 Go 服务的声明式优雅关闭库
</p>

<p align="center">
  <a href="./README.en.md">English</a> ·
  <a href="./README.zh-CN.md">简体中文</a> ·
  <a href="./README.zh-TW.md">繁體中文</a>
</p>

<p align="center">
  Dependency-aware ordering · Timeout orchestration · Panic-safe · Zero dependencies
</p>

---

## Overview

`graceful` is a Go library that encodes the shutdown loop every service needs: drain in-flight requests, stop workers in a controlled order, and exit with a deadlock-free timeout — all with per-task budgets and concurrent execution.

| Feature | Description |
|---|---|
| Per-task timeout + global deadline | Each component gets its own budget, capped by an overall deadline |
| Panic-safe | A panicking `Stop` is recovered and reported |
| Concurrent shutdown | Independent tasks stop in parallel |
| Idempotent | Calling `Shutdown` twice is safe |
| Zero dependencies | Standard library only |
| Cross-platform | `signal.NotifyContext`; defaults to `os.Interrupt` on Windows |

## Install

```sh
go get github.com/jaychouchannel/graceful
```

## Usage

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

err := m.Shutdown()
```

Or use the list-based helper for self-managed shutdown:

```go
ctx, cancel := context.WithCancel(context.Background())
go func() { <-stopCh; cancel() }()

err := graceful.Run(ctx, graceful.Options{}, []graceful.Task{serverTask, dbTask})
```

## Full documentation

- **[English (README.en.md)](./README.en.md)** — API reference, callbacks, testing, Docker, CLI demo, roadmap
- **[简体中文 (README.zh-CN.md)](./README.zh-CN.md)** — 完整中文文档
- **[繁體中文 (README.zh-TW.md)](./README.zh-TW.md)** — 完整繁體中文文檔

## License

[MIT](./LICENSE)
