//go:build !linux

package childsupervisor

import (
	"context"
	"errors"
)

func RunObserve(ctx context.Context, argv []string, opts ObserveOptions) error {
	return errors.New("seccomp user-notification observe mode is only available on Linux")
}

func ObserveExitCode(err error) (int, bool) {
	return 0, false
}
