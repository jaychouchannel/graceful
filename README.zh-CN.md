# graceful

面向 Go 服务的声明式优雅关闭库，支持依赖感知排序与超时编排。

## 为什么不自己写

每个 Go 服务都需要在收到信号时按序排空正在进行的请求、停止工作线程，然后以不会死锁的超时退出。这个库把这些逻辑封装起来，让你无需重复编写：

- **单任务超时 + 全局截止时间。** 每个组件拥有独立的关闭预算，受全局截止时间约束，这样某个卡住的工作线程不会导致整个进程永远等待。
- ** panic 安全。** `Stop` 中发生 panic 会被捕获并上报，而不会导致整个进程崩溃。
- **并发关闭。** 相互独立的任务并行停止，总耗时由最慢的任务决定，而非所有任务耗时之和。
- **幂等。** 多次调用 `Shutdown` 是安全的。
- **零依赖。** 仅使用标准库。
- **跨平台。** 基于 `signal.NotifyContext` 实现。在 Windows 上默认监听 `os.Interrupt`（因为 Windows 不支持 `SIGTERM`）。

## 安装

```sh
go get github.com/jaychouchannel/graceful
```

## 快速开始

在启动时注册组件，在退出时调用 `Shutdown`：

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

// 由 SIGINT/SIGTERM 触发，也可以直接调用。
err := m.Shutdown()
```

默认行为是等待 `SIGINT`/`SIGTERM` 信号后再开始排空（因此在 `main` 函数中调用即可，让进程在外部信号到来时退出）。如果需要主动控制关闭时机，可以使用基于 `context` 的辅助函数：

```go
ctx, cancel := context.WithCancel(context.Background())
go func() {
    // 例如等待自定义的停止条件
    <-stopCh
    cancel()
}()

err := graceful.Run(ctx, graceful.Options{}, []graceful.Task{serverTask, dbTask})
```

## 工作原理

```
SIGINT / SIGTERM / ctx 被取消
        │
        ▼
┌─────────────────────────────────────┐
│  NotifyContext 触发                 │
│  （或 Run 中的 ctx 被取消）        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  对每个已注册任务（并发执行）         │
│  ┌─────────────────────────────────┐ │
│  │  OnTaskStart(name)             │ │
│  │  taskCtx, cancel = WithTimeout │ │
│  │    （截止时间 = ShutdownTimeout）│ │
│  │  Stop(taskCtx)                  │ │
│  │    → 捕获 panic → 上报         │ │
│  │    → 错误 → OnTaskError        │ │
│  │  OnTaskDone                     │ │
│  └─────────────────────────────────┘ │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  WaitGroup.Wait() 或截止时间到达    │
│  若超时 → 返回 ErrTimeout           │
│  若有任务出错 → 聚合错误返回        │
└──────────────┬──────────────────────┘
               │
               ▼
          return err
```

## API 参考

### `NewManager(opts Options) *Manager`

创建一个新的关闭管理器。`ShutdownTimeout` 零值默认为 30 秒；`Signals` 零值在 Windows 上默认为 `[os.Interrupt]`，在 Unix 上默认为 `[os.Interrupt, syscall.SIGTERM]`。

### `(*Manager).Register(task Task)`

注册一个需要在关闭时停止的组件。在 `Shutdown` 触发前或触发后调用都是安全的（在关闭已经开始后注册的任务仍然会被停止）。

### `(*Manager).Shutdown() error`

阻塞等待所有任务停止或全局超时到达。多次调用是安全的——后续调用是空操作并返回 `nil`。如果全局截止时间 exceeded，返回被 `ErrTimeout` 包装的错误；如果任何任务的 `Stop` 返回错误，返回聚合错误。

### `Run(ctx context.Context, opts Options, tasks []Task) error`

便捷函数。创建 `Manager`、注册所有任务并调用 `Shutdown`。当 `ctx` 被取消或收到配置的 OS 信号时触发关闭。

### `Task`

| 字段      | 类型                  | 说明                                              |
|-----------|-----------------------|---------------------------------------------------|
| `Name`    | `string`              | 用于回调和错误信息中的标识符                      |
| `Stop`    | `func(ctx) error`     | 排空组件；收到一个带有单任务超时的 context         |
| `Timeout` | `time.Duration`       | 单任务预算；零值表示使用全局截止时间              |

### `Options`

| 字段              | 默认值                                                   | 说明                                       |
|-------------------|----------------------------------------------------------|--------------------------------------------|
| `ShutdownTimeout` | `30s`                                                    | 所有任务的全局截止时间                     |
| `Signals`         | `[SIGINT, SIGTERM]`（Windows: `[SIGINT]`）              | 触发关闭的信号                             |
| `OnTaskStart`     | `nil`                                                    | 任务开始停止时调用                         |
| `OnTaskError`     | `nil`                                                    | 任务的 `Stop` 返回错误时调用               |

### `ErrTimeout`

当全局 `ShutdownTimeout` exceeded 而所有任务尚未完成时返回（被包装在错误中）。

## 回调

使用 `OnTaskStart` 和 `OnTaskError` 钩入关闭生命周期以记录日志或采集指标：

```go
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 30 * time.Second,
    OnTaskStart: func(name string) {
        log.Printf("正在停止 %s...", name)
    },
    OnTaskError: func(name string, err error) {
        log.Printf("任务 %s 出错: %v", name, err)
    },
})
```

## 测试

`Manager` 暴露了一个内部的 `testTrigger` 通道，用于确定性测试。通过设置 `testTrigger` 可以在不等待真实信号的情况下触发关闭：

```go
trigger := make(chan struct{})
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 5 * time.Second,
})
// 通过内部字段注入 trigger
// （或使用已提前取消的 ctx 调用 Run）
```

完整示例见 `graceful_test.go`。

## CLI 演示

`cmd/graceful` 目录下包含一个演示二进制文件。它运行一个小型 HTTP 服务器、一个后台工作线程和一个模拟数据库，然后在收到 `SIGINT`/`SIGTERM` 时全部关闭：

```sh
go run ./cmd/graceful
# 或构建后运行
go build -o graceful ./cmd/graceful
./graceful
```

## Docker

构建并运行演示容器：

```sh
docker build -t graceful .
docker run -p 8080:8080 graceful
```

`Dockerfile` 使用单阶段 Alpine 构建，并设置 `CGO_ENABLED=0` 以生成静态二进制文件。

## 路线图

- [ ] 依赖图（`DependsOn`）与拓扑排序关闭顺序
- [ ] 基于 `context.WithoutCancel` 的排空上报（正在处理的请求数）
