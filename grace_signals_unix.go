//go:build !windows

package graceful

import (
	"os"
	"syscall"
)

func defaultSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
