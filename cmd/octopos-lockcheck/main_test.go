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

func TestExecutablePathRejectsUnrunnableProcfsTarget(t *testing.T) {
	oldExecutable := osExecutable
	oldArgs := os.Args
	oldPath := os.Getenv("PATH")
	defer func() {
		osExecutable = oldExecutable
		os.Args = oldArgs
		os.Setenv("PATH", oldPath)
	}()

	osExecutable = func() (string, error) {
		return "self", nil
	}
	os.Args = []string{"octopos-lockcheck"}
	os.Setenv("PATH", "")

	got, err := executablePath()
	if err != nil {
		t.Fatalf("executablePath: %v", err)
	}
	if got != "octopos-lockcheck" {
		t.Fatalf("executablePath = %q, want argv[0]", got)
	}
}
