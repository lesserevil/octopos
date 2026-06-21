package remotechild

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestClassifyInheritedFDsDetectsUnsupportedNonStdioFD(t *testing.T) {
	file, err := os.OpenFile(t.TempDir()+"/open-file", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(file.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", file.Fd(), plans)
	}
	if plan.Action != FDActionForceLocal {
		t.Fatalf("action = %s, want %s; plan=%#v", plan.Action, FDActionForceLocal, plan)
	}
	if plan.Kind != FDKindRegular {
		t.Fatalf("kind = %s, want %s; plan=%#v", plan.Kind, FDKindRegular, plan)
	}
	if plan.ReopenPath == "" {
		t.Fatalf("reopen path missing for regular file: %#v", plan)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("prepared action = %s, want %s; plan=%#v", prepared[0].Action, FDActionReopen, prepared[0])
	}
	encoded, err := EncodeReopenFDs(ReopenFDs(prepared))
	if err != nil {
		t.Fatalf("EncodeReopenFDs: %v", err)
	}
	decoded, err := DecodeReopenFDs(encoded)
	if err != nil {
		t.Fatalf("DecodeReopenFDs: %v", err)
	}
	if len(decoded) != 1 || decoded[0].FD != int(file.Fd()) || decoded[0].Path == "" {
		t.Fatalf("decoded reopen fds = %#v", decoded)
	}
}

func TestCurrentFDTargetPrefersHostFDDir(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	fd := 99
	if err := os.Symlink(target, filepath.Join(dir, strconv.Itoa(fd))); err != nil {
		t.Fatalf("symlink host fd: %v", err)
	}
	t.Setenv(EnvHostFDDir, dir)
	if got := currentFDTarget(fd); got != target {
		t.Fatalf("currentFDTarget = %q, want %q", got, target)
	}
}

func TestClassifyInheritedFDsAllowsCloseOnExecFD(t *testing.T) {
	file, err := os.OpenFile(t.TempDir()+"/open-file", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		t.Fatalf("set close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(file.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", file.Fd(), plans)
	}
	if plan.Action != FDActionClose {
		t.Fatalf("action = %s, want %s; plan=%#v", plan.Action, FDActionClose, plan)
	}
	if unsupported := UnsupportedFDs([]FDPlan{plan}); len(unsupported) != 0 {
		t.Fatalf("close-on-exec fd reported unsupported: %#v", unsupported)
	}
}

func TestFormatUnsupportedFDs(t *testing.T) {
	got := FormatUnsupportedFDs([]FDPlan{{
		FD:     5,
		Kind:   FDKindSocket,
		Path:   "socket:[1]",
		Reason: "inherited socket descriptor would not be represented remotely",
	}})
	if !strings.Contains(got, "socket") {
		t.Fatalf("FormatUnsupportedFDs = %q, want socket kind", got)
	}
}

func TestClassifyInheritedFDsReportsPipeKind(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()
	if _, err := unix.FcntlInt(readEnd.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(readEnd.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", readEnd.Fd(), plans)
	}
	if plan.Kind != FDKindPipe {
		t.Fatalf("kind = %s, want %s; plan=%#v", plan.Kind, FDKindPipe, plan)
	}
	if plan.PipeID == "" {
		t.Fatalf("pipe id missing: %#v", plan)
	}
}

func TestClassifyInheritedFDsReportsDeviceNumbers(t *testing.T) {
	file, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(file.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", file.Fd(), plans)
	}
	if plan.Kind != FDKindCharDevice {
		t.Fatalf("kind = %s, want %s; plan=%#v", plan.Kind, FDKindCharDevice, plan)
	}
	if plan.DeviceMajor != 1 || plan.DeviceMinor != 3 {
		t.Fatalf("device = %d:%d, want 1:3; plan=%#v", plan.DeviceMajor, plan.DeviceMinor, plan)
	}
	if plan.ReopenPath != "/dev/null" {
		t.Fatalf("reopen path = %q, want /dev/null; plan=%#v", plan.ReopenPath, plan)
	}
	if !strings.Contains(plan.Reason, "reopened remotely") {
		t.Fatalf("reason = %q, want reopen diagnostic", plan.Reason)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("prepared action = %s, want %s; plan=%#v", prepared[0].Action, FDActionReopen, prepared[0])
	}
}

func TestPrepareFDPlansReopensDevFull(t *testing.T) {
	file, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Skipf("/dev/full unavailable: %v", err)
	}
	defer file.Close()
	clearCloseOnExec(t, file)

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(file.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", file.Fd(), plans)
	}
	if plan.Kind != FDKindCharDevice || plan.ReopenPath != "/dev/full" {
		t.Fatalf("dev full plan = %#v", plan)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("prepared action = %s, want reopen: %#v", prepared[0].Action, prepared[0])
	}
}

func TestClassifyInheritedFDsReportsUnixSocketReason(t *testing.T) {
	left, right := socketPairFiles(t)
	defer left.Close()
	defer right.Close()
	clearCloseOnExec(t, left)

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(left.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", left.Fd(), plans)
	}
	if plan.Kind != FDKindSocket {
		t.Fatalf("kind = %s, want %s; plan=%#v", plan.Kind, FDKindSocket, plan)
	}
	if plan.SocketID == "" {
		t.Fatalf("socket id missing: %#v", plan)
	}
	if !strings.Contains(plan.Reason, "kernel peer state") {
		t.Fatalf("reason = %q, want kernel peer state diagnostic", plan.Reason)
	}
}

func TestClassifyInheritedFDsReportsAnonInodeReasons(t *testing.T) {
	eventFD, err := unix.Eventfd(0, 0)
	if err != nil {
		t.Fatalf("eventfd: %v", err)
	}
	defer unix.Close(eventFD)
	if _, err := unix.FcntlInt(uintptr(eventFD), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear eventfd close-on-exec: %v", err)
	}
	timerFD, err := unix.TimerfdCreate(unix.CLOCK_MONOTONIC, 0)
	if err != nil {
		t.Fatalf("timerfd: %v", err)
	}
	defer unix.Close(timerFD)
	if _, err := unix.FcntlInt(uintptr(timerFD), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear timerfd close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	eventPlan, ok := findPlan(plans, eventFD)
	if !ok {
		t.Fatalf("event fd %d missing from plan: %#v", eventFD, plans)
	}
	if eventPlan.Kind != FDKindAnonInode || !strings.Contains(eventPlan.Reason, "eventfd") {
		t.Fatalf("event plan = %#v", eventPlan)
	}
	timerPlan, ok := findPlan(plans, timerFD)
	if !ok {
		t.Fatalf("timer fd %d missing from plan: %#v", timerFD, plans)
	}
	if timerPlan.Kind != FDKindAnonInode || !strings.Contains(timerPlan.Reason, "timerfd") {
		t.Fatalf("timer plan = %#v", timerPlan)
	}
}

func TestClassifyInheritedFDsReportsSignalfdReason(t *testing.T) {
	var mask unix.Sigset_t
	sig := int(syscall.SIGUSR1) - 1
	mask.Val[sig/64] |= 1 << uint(sig%64)
	fd, err := unix.Signalfd(-1, &mask, 0)
	if err != nil {
		t.Skipf("signalfd unavailable: %v", err)
	}
	defer unix.Close(fd)
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear signalfd close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fd)
	if !ok {
		t.Fatalf("signalfd %d missing from plan: %#v", fd, plans)
	}
	if plan.Kind != FDKindAnonInode || !strings.Contains(plan.Reason, "signalfd") {
		t.Fatalf("signalfd plan = %#v", plan)
	}
}

func TestClassifyInheritedFDsReportsMemfdAsSharedMemory(t *testing.T) {
	fd, err := unix.MemfdCreate("octopos-test", 0)
	if err != nil {
		t.Skipf("memfd unavailable: %v", err)
	}
	defer unix.Close(fd)
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear memfd close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fd)
	if !ok {
		t.Fatalf("memfd %d missing from plan: %#v", fd, plans)
	}
	if !strings.Contains(plan.Reason, "shared-memory") && !strings.Contains(plan.Reason, "deleted") {
		t.Fatalf("memfd reason = %q, want shared-memory/deleted diagnostic; plan=%#v", plan.Reason, plan)
	}
}

func TestClassifyInheritedFDsReportsPidfdReason(t *testing.T) {
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Skipf("pidfd unavailable: %v", err)
	}
	defer unix.Close(fd)
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear pidfd close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fd)
	if !ok {
		t.Fatalf("pidfd %d missing from plan: %#v", fd, plans)
	}
	if plan.Kind != FDKindAnonInode || !strings.Contains(plan.Reason, "pidfd") {
		t.Fatalf("pidfd plan = %#v", plan)
	}
}

func TestClassifyInheritedFDsReportsNetlinkSocketReason(t *testing.T) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW, unix.NETLINK_ROUTE)
	if err != nil {
		t.Skipf("netlink unavailable: %v", err)
	}
	defer unix.Close(fd)
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear netlink close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fd)
	if !ok {
		t.Fatalf("netlink fd %d missing from plan: %#v", fd, plans)
	}
	if plan.Kind != FDKindSocket || !strings.Contains(plan.Reason, "kernel peer state") {
		t.Fatalf("netlink plan = %#v", plan)
	}
}

func TestPrepareFDPlansDoesNotReopenDevShm(t *testing.T) {
	dir := "/dev/shm"
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Skip("/dev/shm unavailable")
	}
	file, err := os.OpenFile(filepath.Join(dir, "octopos-fd-test"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Skipf("open /dev/shm file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	clearCloseOnExec(t, file)

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(file.Fd()))
	if !ok {
		t.Fatalf("/dev/shm fd %d missing from plan: %#v", file.Fd(), plans)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action == FDActionReopen {
		t.Fatalf("/dev/shm fd became reopenable: %#v", prepared[0])
	}
}

func TestClassifyInheritedFDsReportsDeletedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deleted-file")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	clearCloseOnExec(t, file)
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove temp file: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(file.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", file.Fd(), plans)
	}
	if !plan.Deleted {
		t.Fatalf("deleted flag false: %#v", plan)
	}
	if !strings.Contains(plan.Reason, "deleted") {
		t.Fatalf("reason = %q, want deleted diagnostic", plan.Reason)
	}
}

func findPlan(plans []FDPlan, fd int) (FDPlan, bool) {
	for _, plan := range plans {
		if plan.FD == fd {
			return plan, true
		}
	}
	return FDPlan{}, false
}

func clearCloseOnExec(t *testing.T, file *os.File) {
	t.Helper()
	if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}
}

func socketPairFiles(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	listenerPath := filepath.Join(t.TempDir(), "test.sock")
	addr := net.UnixAddr{Name: listenerPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", &addr)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer listener.Close()
	errCh := make(chan error, 1)
	var clientConn *net.UnixConn
	go func() {
		conn, err := net.DialUnix("unix", nil, &addr)
		if err != nil {
			errCh <- err
			return
		}
		clientConn = conn
		errCh <- nil
	}()
	serverConn, err := listener.AcceptUnix()
	if err != nil {
		t.Fatalf("accept unix: %v", err)
	}
	if err := <-errCh; err != nil {
		serverConn.Close()
		t.Fatalf("dial unix: %v", err)
	}
	serverFile, err := serverConn.File()
	serverConn.Close()
	if err != nil {
		clientConn.Close()
		t.Fatalf("server conn file: %v", err)
	}
	clientFile, err := clientConn.File()
	clientConn.Close()
	if err != nil {
		serverFile.Close()
		t.Fatalf("client conn file: %v", err)
	}
	return serverFile, clientFile
}
