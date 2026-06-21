package main

import (
	"os"
	"os/exec"
	"path/filepath"
)

const (
	remoteChildPreloadName       = "liboctopos_remotechild_preload.so"
	remoteChildPreloadSource     = "runtime/remotechild-preload/remotechild_preload.c"
	remoteChildPreloadInstallDir = "/usr/local/lib/octopos"
	remoteChildPreloadPath       = remoteChildPreloadInstallDir + "/" + remoteChildPreloadName
)

func remoteChildPreloadBuildPath(binDir string) string {
	if binDir == "" {
		binDir = "bin"
	}
	return filepath.Join(binDir, remoteChildPreloadName)
}

func ensureRemoteChildPreloadBuildDir(out string) error {
	return os.MkdirAll(filepath.Dir(out), 0755)
}

func remoteChildPreloadBuildCmd(out string) *exec.Cmd {
	cc := os.Getenv("CC")
	if cc == "" {
		cc = "cc"
	}
	return exec.Command(cc,
		"-shared",
		"-fPIC",
		"-O2",
		"-Wall",
		"-Wextra",
		"-o",
		out,
		remoteChildPreloadSource,
		"-ldl",
	)
}
