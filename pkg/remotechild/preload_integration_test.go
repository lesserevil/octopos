package remotechild

import (
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"syscall"
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

func TestPreloadBlocksUnsupportedUnixSocketOperations(t *testing.T) {
	so := buildTestPreload(t)
	probe := buildUnixSocketPolicyProbe(t)
	path := filepath.Join(t.TempDir(), "blocked.sock")

	cmd := exec.Command(probe, path)
	cmd.Env = append(os.Environ(),
		"LD_PRELOAD="+so,
		EnvRemoteChild+"=1",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("unix socket policy probe failed: %v\n%s", err, output)
	}
	got := string(output)
	for _, want := range []string{
		"unix_dgram blocked",
		"socketpair blocked",
		"bind blocked",
		"listen blocked",
		"scm_rights blocked",
		"so_peercred blocked",
		"so_passcred blocked",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("probe output missing %q:\n%s", want, got)
		}
	}
}

func TestPreloadBlocksUnsupportedFIFOOperations(t *testing.T) {
	so := buildTestPreload(t)
	probe := buildFIFOPolicyProbe(t)
	dir := t.TempDir()
	fifoPath := filepath.Join(dir, "existing.fifo")
	if err := syscall.Mkfifo(fifoPath, 0600); err != nil {
		t.Fatalf("mkfifo test fixture: %v", err)
	}
	newFIFOPath := filepath.Join(dir, "new.fifo")

	cmd := exec.Command(probe, fifoPath, newFIFOPath)
	cmd.Env = append(os.Environ(),
		"LD_PRELOAD="+so,
		EnvRemoteChild+"=1",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fifo policy probe failed: %v\n%s", err, output)
	}
	got := string(output)
	for _, want := range []string{
		"open_fifo blocked",
		"openat_fifo blocked",
		"mkfifo blocked",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("probe output missing %q:\n%s", want, got)
		}
	}
}

func TestPreloadBlocksUnsupportedKernelIPCOperations(t *testing.T) {
	so := buildTestPreload(t)
	probe := buildKernelIPCPolicyProbe(t)

	cmd := exec.Command(probe)
	cmd.Env = append(os.Environ(),
		"LD_PRELOAD="+so,
		EnvRemoteChild+"=1",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kernel IPC policy probe failed: %v\n%s", err, output)
	}
	got := string(output)
	for _, want := range []string{
		"eventfd blocked",
		"timerfd blocked",
		"memfd blocked",
		"posix_shm blocked",
		"sysv_shm blocked",
		"sysv_sem blocked",
		"sysv_msg blocked",
		"inotify blocked",
		"fanotify blocked",
		"netlink blocked",
		"ptrace blocked",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("probe output missing %q:\n%s", want, got)
		}
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

func buildUnixSocketPolicyProbe(t *testing.T) string {
	t.Helper()
	if goruntime.GOOS != "linux" {
		t.Skip("remote child preload is Linux-only")
	}
	cc := testCCompiler(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "unix_socket_policy_probe.c")
	bin := filepath.Join(dir, "unix_socket_policy_probe")
	if err := os.WriteFile(src, []byte(unixSocketPolicyProbeSource), 0600); err != nil {
		t.Fatalf("write unix socket policy probe: %v", err)
	}
	cmd := exec.Command(cc, "-O2", "-Wall", "-Wextra", "-o", bin, src)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build unix socket policy probe: %v\n%s", err, output)
	}
	return bin
}

func buildFIFOPolicyProbe(t *testing.T) string {
	t.Helper()
	if goruntime.GOOS != "linux" {
		t.Skip("remote child preload is Linux-only")
	}
	cc := testCCompiler(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "fifo_policy_probe.c")
	bin := filepath.Join(dir, "fifo_policy_probe")
	if err := os.WriteFile(src, []byte(fifoPolicyProbeSource), 0600); err != nil {
		t.Fatalf("write fifo policy probe: %v", err)
	}
	cmd := exec.Command(cc, "-O2", "-Wall", "-Wextra", "-o", bin, src)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fifo policy probe: %v\n%s", err, output)
	}
	return bin
}

func buildKernelIPCPolicyProbe(t *testing.T) string {
	t.Helper()
	if goruntime.GOOS != "linux" {
		t.Skip("remote child preload is Linux-only")
	}
	cc := testCCompiler(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "kernel_ipc_policy_probe.c")
	bin := filepath.Join(dir, "kernel_ipc_policy_probe")
	if err := os.WriteFile(src, []byte(kernelIPCPolicyProbeSource), 0600); err != nil {
		t.Fatalf("write kernel IPC policy probe: %v", err)
	}
	cmd := exec.Command(cc, "-O2", "-Wall", "-Wextra", "-o", bin, src)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build kernel IPC policy probe: %v\n%s", err, output)
	}
	return bin
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

const unixSocketPolicyProbeSource = `
#define _GNU_SOURCE
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static int expect_enotsup(const char *name, int rc) {
    if (rc == -1 && errno == ENOTSUP) {
        printf("%s blocked\n", name);
        return 0;
    }
    fprintf(stderr, "%s: rc=%d errno=%d\n", name, rc, errno);
    return 1;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        return 2;
    }
    int failures = 0;

    errno = 0;
    int dgram = socket(AF_UNIX, SOCK_DGRAM, 0);
    failures += expect_enotsup("unix_dgram", dgram);
    if (dgram >= 0) {
        close(dgram);
    }

    int sv[2] = {-1, -1};
    errno = 0;
    failures += expect_enotsup("socketpair", socketpair(AF_UNIX, SOCK_STREAM, 0, sv));
    if (sv[0] >= 0) {
        close(sv[0]);
    }
    if (sv[1] >= 0) {
        close(sv[1]);
    }

    int stream = socket(AF_UNIX, SOCK_STREAM, 0);
    if (stream < 0) {
        perror("stream socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", argv[1]);
    unlink(addr.sun_path);
    errno = 0;
    failures += expect_enotsup("bind", bind(stream, (struct sockaddr *)&addr, sizeof(addr)));

    errno = 0;
    failures += expect_enotsup("listen", listen(stream, 1));

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &stream, sizeof(int));
    errno = 0;
    failures += expect_enotsup("scm_rights", (int)sendmsg(stream, &msg, 0));

    struct ucred cred;
    socklen_t cred_len = sizeof(cred);
    errno = 0;
    failures += expect_enotsup("so_peercred", getsockopt(stream, SOL_SOCKET, SO_PEERCRED, &cred, &cred_len));

    int one = 1;
    errno = 0;
    failures += expect_enotsup("so_passcred", setsockopt(stream, SOL_SOCKET, SO_PASSCRED, &one, sizeof(one)));

    close(stream);
    return failures == 0 ? 0 : 1;
}
`

const fifoPolicyProbeSource = `
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

static int expect_enotsup(const char *name, int rc) {
    if (rc == -1 && errno == ENOTSUP) {
        printf("%s blocked\n", name);
        return 0;
    }
    fprintf(stderr, "%s: rc=%d errno=%d\n", name, rc, errno);
    return 1;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        return 2;
    }
    int failures = 0;

    errno = 0;
    int fd = open(argv[1], O_RDONLY | O_NONBLOCK);
    failures += expect_enotsup("open_fifo", fd);
    if (fd >= 0) {
        close(fd);
    }

    errno = 0;
    fd = openat(AT_FDCWD, argv[1], O_RDONLY | O_NONBLOCK);
    failures += expect_enotsup("openat_fifo", fd);
    if (fd >= 0) {
        close(fd);
    }

    errno = 0;
    failures += expect_enotsup("mkfifo", mkfifo(argv[2], 0600));

    return failures == 0 ? 0 : 1;
}
`

const kernelIPCPolicyProbeSource = `
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/netlink.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/fanotify.h>
#include <sys/inotify.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/msg.h>
#include <sys/ptrace.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

static int expect_enotsup(const char *name, long rc) {
    if (rc == -1 && errno == ENOTSUP) {
        printf("%s blocked\n", name);
        return 0;
    }
    fprintf(stderr, "%s: rc=%ld errno=%d\n", name, rc, errno);
    return 1;
}

int main(void) {
    int failures = 0;

    errno = 0;
    failures += expect_enotsup("eventfd", eventfd(0, 0));

    errno = 0;
    failures += expect_enotsup("timerfd", timerfd_create(CLOCK_MONOTONIC, 0));

    errno = 0;
    failures += expect_enotsup("memfd", memfd_create("octopos-probe", 0));

    errno = 0;
    failures += expect_enotsup("posix_shm", shm_open("/octopos-probe", O_CREAT | O_RDWR, 0600));

    errno = 0;
    failures += expect_enotsup("sysv_shm", shmget(IPC_PRIVATE, 4096, IPC_CREAT | 0600));

    errno = 0;
    failures += expect_enotsup("sysv_sem", semget(IPC_PRIVATE, 1, IPC_CREAT | 0600));

    errno = 0;
    failures += expect_enotsup("sysv_msg", msgget(IPC_PRIVATE, IPC_CREAT | 0600));

    errno = 0;
    failures += expect_enotsup("inotify", inotify_init1(0));

    errno = 0;
    failures += expect_enotsup("fanotify", fanotify_init(0, 0));

    errno = 0;
    failures += expect_enotsup("netlink", socket(AF_NETLINK, SOCK_RAW, NETLINK_ROUTE));

    errno = 0;
    failures += expect_enotsup("ptrace", ptrace(PTRACE_TRACEME, 0, NULL, NULL));

    return failures == 0 ? 0 : 1;
}
`
