# graceful

面向 Go 服務的宣告式優雅關閉庫，支援依賴感知排序與超時編排。

## 為什麼不自己寫

每個 Go 服務都需要在收到信號時按序排空正在進行的請求、停止工作線程，然後以不會死鎖的超時退出。這個庫把這些邏輯封裝起來，讓你無需重複編寫：

- **單任務超時 + 全局截止時間。** 每個組件擁有獨立的關閉預算，受全局截止時間約束，這樣某個卡住的工作線程不會導致整個進程永遠等待。
- **panic 安全。** `Stop` 中發生 panic 會被捕獲並上報，而不會導致整個進程崩潰。
- **並發關閉。** 相互獨立的任務並行停止，總耗時由最慢的任務決定，而非所有任務耗時之和。
- **冪等。** 多次調用 `Shutdown` 是安全的。
- **零依賴。** 僅使用標準庫。
- **跨平台。** 基於 `signal.NotifyContext` 實現。在 Windows 上默認監聽 `os.Interrupt`（因為 Windows 不支援 `SIGTERM`）。

## 安裝

```sh
go get github.com/jaychouchannel/graceful
```

## 快速開始

在啟動時註冊組件，在退出時調用 `Shutdown`：

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

// 由 SIGINT/SIGTERM 觸發，也可以直接調用。
err := m.Shutdown()
```

預設行為是等待 `SIGINT`/`SIGTERM` 信號後再開始排空（因此在 `main` 函式中調用即可，讓進程在外部信號到來時退出）。如果需要主動控制關閉時機，可以使用基於 `context` 的輔助函式：

```go
ctx, cancel := context.WithCancel(context.Background())
go func() {
    // 例如等待自定義的停止條件
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
│  NotifyContext 觸發                 │
│  （或 Run 中的 ctx 被取消）        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  對每個已註冊任務（並發執行）         │
│  ┌─────────────────────────────────┐ │
│  │  OnTaskStart(name)             │ │
│  │  taskCtx, cancel = WithTimeout │ │
│  │    （截止時間 = ShutdownTimeout）│ │
│  │  Stop(taskCtx)                  │ │
│  │    → 捕獲 panic → 上報         │ │
│  │    → 錯誤 → OnTaskError        │ │
│  │  OnTaskDone                     │ │
│  └─────────────────────────────────┘ │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  WaitGroup.Wait() 或截止時間到達    │
│  若超時 → 返回 ErrTimeout           │
│  若有任務出錯 → 聚合錯誤返回        │
└──────────────┬──────────────────────┘
               │
               ▼
          return err
```

## API 參考

### `NewManager(opts Options) *Manager`

創建一個新的關閉管理器。`ShutdownTimeout` 零值預設為 30 秒；`Signals` 零值在 Windows 上預設為 `[os.Interrupt]`，在 Unix 上預設為 `[os.Interrupt, syscall.SIGTERM]`。

### `(*Manager).Register(task Task)`

註冊一個需要在關閉時停止的組件。在 `Shutdown` 觸發前或觸發後調用都是安全的（在關閉已經開始後註冊的任務仍然會被停止）。

### `(*Manager).Shutdown() error`

阻塞等待所有任務停止或全局超時到達。多次調用是安全的——後續調用是空操作並返回 `nil`。如果全局截止時間 exceeded，返回被 `ErrTimeout` 包裝的錯誤；如果任何任務的 `Stop` 返回錯誤，返回聚合錯誤。

### `Run(ctx context.Context, opts Options, tasks []Task) error`

便捷函式。創建 `Manager`、註冊所有任務並調用 `Shutdown`。當 `ctx` 被取消或收到配置的 OS 信號時觸發關閉。

### `Task`

| 字段      | 類型                  | 說明                                              |
|-----------|-----------------------|---------------------------------------------------|
| `Name`    | `string`              | 用於回調和錯誤資訊中的標識符                      |
| `Stop`    | `func(ctx) error`     | 排空組件；收到一個帶有單任務超時的 context         |
| `Timeout` | `time.Duration`       | 單任務預算；零值表示使用全局截止時間              |

### `Options`

| 字段              | 預設值                                                   | 說明                                       |
|-------------------|----------------------------------------------------------|--------------------------------------------|
| `ShutdownTimeout` | `30s`                                                    | 所有任務的全局截止時間                     |
| `Signals`         | `[SIGINT, SIGTERM]`（Windows: `[SIGINT]`）              | 觸發關閉的信號                             |
| `OnTaskStart`     | `nil`                                                    | 任務開始停止時調用                         |
| `OnTaskError`     | `nil`                                                    | 任務的 `Stop` 返回錯誤時調用               |

### `ErrTimeout`

當全局 `ShutdownTimeout` exceeded 而所有任務尚未完成時返回（被包裝在錯誤中）。

## 回調

使用 `OnTaskStart` 和 `OnTaskError` 鉤入關閉生命週期以記錄日誌或採集指標：

```go
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 30 * time.Second,
    OnTaskStart: func(name string) {
        log.Printf("正在停止 %s...", name)
    },
    OnTaskError: func(name string, err error) {
        log.Printf("任務 %s 出錯: %v", name, err)
    },
})
```

## 測試

`Manager` 暴露了一個內部的 `testTrigger` 通道，用於確定性測試。通過設置 `testTrigger` 可以在不等待真實信號的情況下觸發關閉：

```go
trigger := make(chan struct{})
m := graceful.NewManager(graceful.Options{
    ShutdownTimeout: 5 * time.Second,
})
// 通過內部字段注入 trigger
// （或使用已提前取消的 ctx 調用 Run）
```

完整示例見 `graceful_test.go`。

## CLI 演示

`cmd/graceful` 目錄下包含一個演示二進位檔。它運行一個小型 HTTP 伺服器、一個後台工作線程和一個模擬資料庫，然後在收到 `SIGINT`/`SIGTERM` 時全部關閉：

```sh
go run ./cmd/graceful
# 或構建後運行
go build -o graceful ./cmd/graceful
./graceful
```

## Docker

構建並運行演示容器：

```sh
docker build -t graceful .
docker run -p 8080:8080 graceful
```

`Dockerfile` 使用單階段 Alpine 構建，並設置 `CGO_ENABLED=0` 以生成靜態二進位檔。

## 路線圖

- [ ] 依賴圖（`DependsOn`）與拓撲排序關閉順序
- [ ] 基於 `context.WithoutCancel` 的排空上報（正在處理的請求數）
