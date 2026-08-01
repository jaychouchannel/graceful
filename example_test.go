package graceful_test

import (
	"context"
	"fmt"
	"time"

	"github.com/jaychouchannel/graceful"
)

func ExampleRun() {
	tasks := []graceful.Task{
		{
			Name: "worker",
			Stop: func(ctx context.Context) error {
				fmt.Println("worker stopped")
				return nil
			},
			Timeout: 5 * time.Second,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Run does not wait for a signal; it returns when ctx is cancelled.
	err := graceful.Run(ctx, graceful.Options{}, tasks)
	if err != nil {
		fmt.Println("error:", err)
	}

	// Output: worker stopped
}
