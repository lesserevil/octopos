package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"github.com/octopos/octopos/pkg/childsupervisor"
)

func main() {
	checkOnly := flag.Bool("check", false, "Print seccomp user-notification support and exit")
	require := flag.Bool("require", false, "Exit non-zero when production supervisor support is unavailable")
	observe := flag.Bool("observe", false, "Run the command under an observe-only seccomp user-notification loop")
	flag.Parse()

	report := childsupervisor.CheckSupport()
	if *checkOnly || flag.NArg() == 0 {
		fmt.Println(report.String())
		if *require && !report.ProductionSupervisorUsable {
			os.Exit(1)
		}
		return
	}
	if *require && !report.ProductionSupervisorUsable {
		fmt.Fprintln(os.Stderr, "octopos-child-supervisor: seccomp user notification is unavailable")
		os.Exit(1)
	}

	var err error
	if *observe {
		err = childsupervisor.RunObserve(context.Background(), flag.Args(), childsupervisor.ObserveOptions{Log: os.Stderr})
	} else {
		err = execLocal(flag.Args())
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "octopos-child-supervisor: %v\n", err)
		if code, ok := childsupervisor.ObserveExitCode(err); ok {
			os.Exit(code)
		}
		if exitErr := new(exec.ExitError); errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		os.Exit(1)
	}
}

func execLocal(argv []string) error {
	if len(argv) == 0 {
		return nil
	}
	path, err := exec.LookPath(argv[0])
	if err != nil {
		return err
	}
	return syscall.Exec(path, argv, os.Environ())
}
