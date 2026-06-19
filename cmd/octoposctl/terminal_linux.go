//go:build linux

package main

import (
	"syscall"
	"unsafe"
)

type terminalState struct {
	termios syscall.Termios
}

type terminalSize struct {
	rows uint16
	cols uint16
}

func isTerminal(fd uintptr) bool {
	_, err := getTermios(fd)
	return err == nil
}

func makeTerminalRaw(fd uintptr) (*terminalState, error) {
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
	return &terminalState{termios: oldState}, nil
}

func restoreTerminal(fd uintptr, state *terminalState) {
	if state == nil {
		return
	}
	_ = setTermios(fd, state.termios)
}

func getTerminalSize(fd uintptr) (terminalSize, error) {
	var ws struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 {
		return terminalSize{}, errno
	}
	return terminalSize{rows: ws.Row, cols: ws.Col}, nil
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
