package lockcheck

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type Kind string

const (
	KindFlock Kind = "flock"
	KindFcntl Kind = "fcntl"
)

var ErrLocked = errors.New("lock is already held")

func ValidateKind(kind Kind) error {
	switch kind {
	case KindFlock, KindFcntl:
		return nil
	default:
		return fmt.Errorf("unsupported lock kind %q", kind)
	}
}

func OpenLockFile(path string) (*os.File, error) {
	if path == "" {
		return nil, errors.New("missing lock path")
	}
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
}

func Lock(file *os.File, kind Kind, nonblock bool) error {
	if file == nil {
		return errors.New("nil lock file")
	}
	if err := ValidateKind(kind); err != nil {
		return err
	}
	switch kind {
	case KindFlock:
		flags := unix.LOCK_EX
		if nonblock {
			flags |= unix.LOCK_NB
		}
		err := unix.Flock(int(file.Fd()), flags)
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return ErrLocked
		}
		return err
	case KindFcntl:
		lock := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: int16(os.SEEK_SET),
			Start:  0,
			Len:    0,
		}
		cmd := unix.F_SETLKW
		if nonblock {
			cmd = unix.F_SETLK
		}
		err := unix.FcntlFlock(file.Fd(), cmd, &lock)
		if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EAGAIN) {
			return ErrLocked
		}
		return err
	default:
		return fmt.Errorf("unsupported lock kind %q", kind)
	}
}

func Unlock(file *os.File, kind Kind) error {
	if file == nil {
		return nil
	}
	switch kind {
	case KindFlock:
		return unix.Flock(int(file.Fd()), unix.LOCK_UN)
	case KindFcntl:
		lock := unix.Flock_t{
			Type:   unix.F_UNLCK,
			Whence: int16(os.SEEK_SET),
			Start:  0,
			Len:    0,
		}
		return unix.FcntlFlock(file.Fd(), unix.F_SETLK, &lock)
	default:
		return fmt.Errorf("unsupported lock kind %q", kind)
	}
}
