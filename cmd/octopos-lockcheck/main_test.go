package main

import (
	"errors"
	"os"
	"testing"
)

func TestExecutablePathFallsBackToArgv0(t *testing.T) {
	oldExecutable := osExecutable
	oldArgs := os.Args
	defer func() {
		osExecutable = oldExecutable
		os.Args = oldArgs
	}()

	osExecutable = func() (string, error) {
		return "", errors.New("no proc exe")
	}
	os.Args = []string{"octopos-lockcheck"}

	got, err := executablePath()
	if err != nil {
		t.Fatalf("executablePath: %v", err)
	}
	if got != "octopos-lockcheck" {
		t.Fatalf("executablePath = %q, want argv[0]", got)
	}
}
