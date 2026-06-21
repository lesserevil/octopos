//go:build linux

package termio

import (
	"syscall"
	"unsafe"
)

type State struct {
	termios syscall.Termios
}

type Size struct {
	Rows uint16
	Cols uint16
}

func IsTerminal(fd uintptr) bool {
	_, err := getTermios(fd)
	return err == nil
}

func MakeRaw(fd uintptr) (*State, error) {
	oldState, err := getTermios(fd)
	if err != nil {
		return nil, err
	}

	raw := oldState
	raw.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK | syscall.ISTRIP | syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON
	raw.Oflag &^= syscall.OPOST
	raw.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	raw.Cflag &^= syscall.CSIZE | syscall.PARENB
	raw.Cflag |= syscall.CS8
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0

	if err := setTermios(fd, raw); err != nil {
		return nil, err
	}
	return &State{termios: oldState}, nil
}

func Restore(fd uintptr, state *State) {
	if state == nil {
		return
	}
	_ = setTermios(fd, state.termios)
}

func GetSize(fd uintptr) (Size, error) {
	var ws struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 {
		return Size{}, errno
	}
	return Size{Rows: ws.Row, Cols: ws.Col}, nil
}

func getTermios(fd uintptr) (syscall.Termios, error) {
	var termios syscall.Termios
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(syscall.TCGETS), uintptr(unsafe.Pointer(&termios)))
	if errno != 0 {
		return syscall.Termios{}, errno
	}
	return termios, nil
}

func setTermios(fd uintptr, termios syscall.Termios) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(syscall.TCSETS), uintptr(unsafe.Pointer(&termios)))
	if errno != 0 {
		return errno
	}
	return nil
}
