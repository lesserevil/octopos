package remotechild

import (
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
)

func TestPreloadRelaxedIPCCompatConvertsReadOnlyFileMapping(t *testing.T) {
	so := buildTestPreload(t)
	probe := buildMMapProbe(t)
	path := writeMMapProbeFile(t, filepath.Join(t.TempDir(), "data"))

	stdout, stderr, err := runMMapProbe(t, probe, so, "readonly", path)
	if err != nil {
		t.Fatalf("probe readonly failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if strings.TrimSpace(stdout) != "x" {
		t.Fatalf("stdout = %q, want x", stdout)
	}
	if !strings.Contains(stderr, "converted read-only MAP_SHARED mapping to MAP_PRIVATE") {
		t.Fatalf("stderr missing relaxed conversion warning:\n%s", stderr)
	}
}

func TestPreloadRelaxedIPCCompatBlocksWritableSharedMapping(t *testing.T) {
	so := buildTestPreload(t)
	probe := buildMMapProbe(t)
	path := writeMMapProbeFile(t, filepath.Join(t.TempDir(), "data"))

	stdout, stderr, err := runMMapProbe(t, probe, so, "writable", path)
	if err != nil {
		t.Fatalf("probe writable failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if strings.TrimSpace(stdout) != "blocked" {
		t.Fatalf("stdout = %q, want blocked", stdout)
	}
	if !strings.Contains(stderr, "writable MAP_SHARED") {
		t.Fatalf("stderr missing writable block reason:\n%s", stderr)
	}
}

func TestPreloadRelaxedIPCCompatBlocksDevShmMapping(t *testing.T) {
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skipf("/dev/shm unavailable: %v", err)
	}

	so := buildTestPreload(t)
	probe := buildMMapProbe(t)
	path := writeMMapProbeFile(t, filepath.Join("/dev/shm", "octopos-mmap-probe-"+preloadTestSuffix(t)))
	defer os.Remove(path)

	stdout, stderr, err := runMMapProbe(t, probe, so, "readonly-blocked", path)
	if err != nil {
		t.Fatalf("probe /dev/shm failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if strings.TrimSpace(stdout) != "blocked" {
		t.Fatalf("stdout = %q, want blocked", stdout)
	}
	if !strings.Contains(stderr, "shared-memory MAP_SHARED") {
		t.Fatalf("stderr missing shared-memory block reason:\n%s", stderr)
	}
}

func TestPreloadWrapsSystem(t *testing.T) {
	so := buildTestPreload(t)
	probe := buildSystemProbe(t)
	dir := t.TempDir()
	helper := filepath.Join(dir, "octopos-remote-child")
	logPath := filepath.Join(dir, "helper.log")
	helperScript := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$OCTOPOS_SYSTEM_PROBE_LOG\"\nexit 0\n"
	if err := os.WriteFile(helper, []byte(helperScript), 0700); err != nil {
		t.Fatalf("write helper: %v", err)
	}

	cmd := exec.Command(probe)
	cmd.Env = append(os.Environ(),
		"LD_PRELOAD="+so,
		"OCTOPOS_REMOTE_CHILDREN=safe",
		"OCTOPOS_REMOTE_CHILD_PATH="+helper,
		"OCTOPOS_SYSTEM_PROBE_LOG="+logPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("system probe failed: %v\n%s", err, output)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read helper log: %v", err)
	}
	got := strings.Split(strings.TrimSpace(string(data)), "\n")
	want := []string{"--", "/bin/sh", "-c", "printf system-probe"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("helper args = %#v, want %#v", got, want)
	}
}

func buildTestPreload(t *testing.T) string {
	t.Helper()
	if goruntime.GOOS != "linux" {
		t.Skip("remote child preload is Linux-only")
	}
	cc := testCCompiler(t)
	out := filepath.Join(t.TempDir(), "liboctopos_remotechild_preload.so")
	cmd := exec.Command(cc, "-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-o", out, preloadSourcePath(t), "-ldl")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build preload: %v\n%s", err, output)
	}
	return out
}

func preloadSourcePath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "runtime", "remotechild-preload", "remotechild_preload.c"))
}

func buildMMapProbe(t *testing.T) string {
	t.Helper()
	cc := testCCompiler(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "mmap_probe.c")
	bin := filepath.Join(dir, "mmap_probe")
	if err := os.WriteFile(src, []byte(mmapProbeSource), 0600); err != nil {
		t.Fatalf("write mmap probe: %v", err)
	}
	cmd := exec.Command(cc, "-O2", "-Wall", "-Wextra", "-o", bin, src)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build mmap probe: %v\n%s", err, output)
	}
	return bin
}

func testCCompiler(t *testing.T) string {
	t.Helper()
	cc := os.Getenv("CC")
	if cc == "" {
		cc = "cc"
	}
	path, err := exec.LookPath(cc)
	if err != nil {
		t.Skipf("C compiler %q unavailable: %v", cc, err)
	}
	return path
}

func buildSystemProbe(t *testing.T) string {
	t.Helper()
	if goruntime.GOOS != "linux" {
		t.Skip("remote child preload is Linux-only")
	}
	cc := testCCompiler(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "system_probe.c")
	bin := filepath.Join(dir, "system_probe")
	if err := os.WriteFile(src, []byte(systemProbeSource), 0600); err != nil {
		t.Fatalf("write system probe: %v", err)
	}
	cmd := exec.Command(cc, "-O2", "-Wall", "-Wextra", "-o", bin, src)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build system probe: %v\n%s", err, output)
	}
	return bin
}

func writeMMapProbeFile(t *testing.T, path string) string {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"+strings.Repeat("0", 4095)), 0600); err != nil {
		t.Fatalf("write probe file: %v", err)
	}
	return path
}

func preloadTestSuffix(t *testing.T) string {
	t.Helper()
	file, err := os.CreateTemp("", "suffix-*")
	if err != nil {
		t.Fatalf("create temp suffix: %v", err)
	}
	name := filepath.Base(file.Name())
	_ = file.Close()
	_ = os.Remove(file.Name())
	return name
}

func runMMapProbe(t *testing.T, probe string, so string, mode string, path string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(probe, mode, path)
	cmd.Env = append(os.Environ(),
		"LD_PRELOAD="+so,
		EnvRemoteChild+"=1",
		EnvIPCCompat+"=relaxed",
	)
	var stdout strings.Builder
	var stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

const mmapProbeSource = `
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <readonly|readonly-blocked|writable> <path>\n", argv[0]);
        return 2;
    }

    int expect_blocked = strcmp(argv[1], "readonly-blocked") == 0 || strcmp(argv[1], "writable") == 0;
    int prot = PROT_READ;
    int open_flags = O_RDONLY;
    if (strcmp(argv[1], "writable") == 0) {
        prot |= PROT_WRITE;
        open_flags = O_RDWR;
    }

    int fd = open(argv[2], open_flags);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    void *mapped = mmap(NULL, 4096, prot, MAP_SHARED, fd, 0);
    if (expect_blocked) {
        if (mapped == MAP_FAILED && errno == ENOTSUP) {
            puts("blocked");
            close(fd);
            return 0;
        }
        fprintf(stderr, "expected ENOTSUP, got mapped=%p errno=%d\n", mapped, errno);
        if (mapped != MAP_FAILED) {
            munmap(mapped, 4096);
        }
        close(fd);
        return 1;
    }
    if (mapped == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return 1;
    }

    printf("%c\n", ((char *)mapped)[0]);
    munmap(mapped, 4096);
    close(fd);
    return 0;
}
`

const systemProbeSource = `
#include <stdlib.h>
#include <sys/wait.h>

int main(void) {
    int status = system("printf system-probe");
    if (status == -1) {
        return 2;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        return 3;
    }
    return 0;
}
`
