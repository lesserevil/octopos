//go:build !linux

package termio

import "errors"

type State struct{}

type Size struct {
	Rows uint16
	Cols uint16
}

func IsTerminal(fd uintptr) bool {
	return false
}

func MakeRaw(fd uintptr) (*State, error) {
	return nil, errors.New("terminal raw mode is only supported on linux")
}

func Restore(fd uintptr, state *State) {}

func GetSize(fd uintptr) (Size, error) {
	return Size{}, errors.New("terminal size is only supported on linux")
}
