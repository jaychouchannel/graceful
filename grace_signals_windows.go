//go:build windows

package graceful

import "os"

func defaultSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
