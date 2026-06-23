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
		FD:         5,
		Kind:       FDKindSocket,
		Path:       "socket:[1]",
		Reason:     "inherited socket descriptor would not be represented remotely",
		ReasonCode: FDReasonSocket,
	}})
	if !strings.Contains(got, "socket") {
		t.Fatalf("FormatUnsupportedFDs = %q, want socket kind", got)
	}
	if !strings.Contains(got, string(FDReasonSocket)) {
		t.Fatalf("FormatUnsupportedFDs = %q, want reason code", got)
	}
	codes := FormatUnsupportedReasonCodes([]FDPlan{
		{ReasonCode: FDReasonSocket},
		{ReasonCode: FDReasonSocket},
		{ReasonCode: FDReasonEventFD},
	})
	if codes != string(FDReasonEventFD)+","+string(FDReasonSocket) {
		t.Fatalf("FormatUnsupportedReasonCodes = %q", codes)
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
	if plan.ReasonCode != FDReasonPipe {
		t.Fatalf("reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonPipe, plan)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowPipeProxy: true})
	if prepared[0].Action != FDActionForceLocal {
		t.Fatalf("non-stdio pipe fd action = %s, want %s; plan=%#v", prepared[0].Action, FDActionForceLocal, prepared[0])
	}
}

func TestPrepareFDPlansAllowsStdioPipeProxy(t *testing.T) {
	plan := FDPlan{
		FD:         1,
		Kind:       FDKindPipe,
		Action:     FDActionForceLocal,
		Path:       "pipe:[123]",
		PipeID:     "123",
		Reason:     "anonymous pipe requires coordinated pipe proxying",
		ReasonCode: FDReasonPipe,
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowPipeProxy: true})
	if prepared[0].Action != FDActionProxyStream {
		t.Fatalf("stdio pipe fd action = %s, want %s; plan=%#v", prepared[0].Action, FDActionProxyStream, prepared[0])
	}
}

func TestClassifyInheritedFDsReportsNamedFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.fifo")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NONBLOCK, 0600)
	if err != nil {
		t.Fatalf("open fifo: %v", err)
	}
	defer unix.Close(fd)
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fd)
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", fd, plans)
	}
	if plan.Kind != FDKindPipe || plan.FIFOPath != path {
		t.Fatalf("fifo plan = %#v", plan)
	}
	if plan.ReasonCode != FDReasonFIFO {
		t.Fatalf("reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonFIFO, plan)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowFIFOProxy: true})
	if prepared[0].Action != FDActionForceLocal {
		t.Fatalf("non-stdio FIFO action = %s, want %s; plan=%#v", prepared[0].Action, FDActionForceLocal, prepared[0])
	}
}

func TestPrepareFDPlansAllowsStdioFIFOProxy(t *testing.T) {
	plan := FDPlan{
		FD:         0,
		Kind:       FDKindPipe,
		Action:     FDActionForceLocal,
		Path:       "/cluster/tmp/fifo",
		FIFOPath:   "/cluster/tmp/fifo",
		Reason:     "named FIFO requires coordinated FIFO broker semantics",
		ReasonCode: FDReasonFIFO,
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowFIFOProxy: true})
	if prepared[0].Action != FDActionProxyStream {
		t.Fatalf("stdio FIFO action = %s, want %s; plan=%#v", prepared[0].Action, FDActionProxyStream, prepared[0])
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
	if plan.ReasonCode != FDReasonCharDeviceReopenable {
		t.Fatalf("reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonCharDeviceReopenable, plan)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("prepared action = %s, want %s; plan=%#v", prepared[0].Action, FDActionReopen, prepared[0])
	}
	if prepared[0].ReasonCode != FDReasonRemoteReopen {
		t.Fatalf("prepared reason code = %q, want %q; plan=%#v", prepared[0].ReasonCode, FDReasonRemoteReopen, prepared[0])
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

func TestParseDeviceAllowlist(t *testing.T) {
	rules, err := ParseDeviceAllowlist("/dev/fuse,char:195:0,block:8:1,1:3")
	if err != nil {
		t.Fatalf("ParseDeviceAllowlist: %v", err)
	}
	if len(rules) != 4 {
		t.Fatalf("rules = %#v, want four", rules)
	}
	if rules[0].Path != "/dev/fuse" {
		t.Fatalf("path rule = %#v", rules[0])
	}
	if rules[1].Kind != FDKindCharDevice || !rules[1].HasDevice || rules[1].Major != 195 || rules[1].Minor != 0 {
		t.Fatalf("char rule = %#v", rules[1])
	}
	if rules[2].Kind != FDKindBlockDevice || !rules[2].HasDevice || rules[2].Major != 8 || rules[2].Minor != 1 {
		t.Fatalf("block rule = %#v", rules[2])
	}
	if rules[3].Kind != "" || !rules[3].HasDevice || rules[3].Major != 1 || rules[3].Minor != 3 {
		t.Fatalf("major/minor rule = %#v", rules[3])
	}
	if _, err := ParseDeviceAllowlist("bad"); err == nil {
		t.Fatal("ParseDeviceAllowlist accepted invalid entry")
	}
}

func TestPrepareFDPlansReopensAllowlistedCharacterDevice(t *testing.T) {
	plan := FDPlan{
		FD:          7,
		Kind:        FDKindCharDevice,
		Action:      FDActionForceLocal,
		Path:        "/dev/fuse",
		ReopenPath:  "/dev/fuse",
		DeviceMajor: 10,
		DeviceMinor: 229,
		Reason:      "character device descriptor requires an explicit device allowlist",
		ReasonCode:  FDReasonCharDeviceAllowlist,
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action == FDActionReopen {
		t.Fatalf("device reopened without allowlist: %#v", prepared[0])
	}

	rules, err := ParseDeviceAllowlist("/dev/fuse")
	if err != nil {
		t.Fatalf("ParseDeviceAllowlist: %v", err)
	}
	prepared = PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true, AllowedDevices: rules})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("path-allowlisted device was not reopened: %#v", prepared[0])
	}

	rules, err = ParseDeviceAllowlist("char:10:229")
	if err != nil {
		t.Fatalf("ParseDeviceAllowlist: %v", err)
	}
	prepared = PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true, AllowedDevices: rules})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("major/minor-allowlisted device was not reopened: %#v", prepared[0])
	}
}

func TestPrepareFDPlansReopensAllowlistedBlockDevice(t *testing.T) {
	plan := FDPlan{
		FD:          7,
		Kind:        FDKindBlockDevice,
		Action:      FDActionForceLocal,
		Path:        "/dev/loop0",
		ReopenPath:  "/dev/loop0",
		DeviceMajor: 7,
		DeviceMinor: 0,
		Reason:      "block device descriptor requires an explicit device allowlist",
		ReasonCode:  FDReasonBlockDeviceAllowlist,
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action == FDActionReopen {
		t.Fatalf("block device reopened without allowlist: %#v", prepared[0])
	}
	rules, err := ParseDeviceAllowlist("block:7:0")
	if err != nil {
		t.Fatalf("ParseDeviceAllowlist: %v", err)
	}
	prepared = PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true, AllowedDevices: rules})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("allowlisted block device was not reopened: %#v", prepared[0])
	}
}

func TestPrepareFDPlansRequiresGPUAllocationForNVIDIADevices(t *testing.T) {
	plan := FDPlan{
		FD:          7,
		Kind:        FDKindCharDevice,
		Action:      FDActionForceLocal,
		Path:        "/dev/nvidia0",
		ReopenPath:  "/dev/nvidia0",
		DeviceMajor: 195,
		DeviceMinor: 0,
		Reason:      "NVIDIA device descriptor requires an allocated GPU for remote reopening",
		ReasonCode:  FDReasonNVIDIARequiresGPU,
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action == FDActionReopen {
		t.Fatalf("NVIDIA device reopened without GPU allocation: %#v", prepared[0])
	}

	rules, err := ParseDeviceAllowlist("/dev/nvidia0,char:195:0")
	if err != nil {
		t.Fatalf("ParseDeviceAllowlist: %v", err)
	}
	prepared = PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true, AllowedDevices: rules})
	if prepared[0].Action == FDActionReopen {
		t.Fatalf("NVIDIA device reopened through generic device allowlist: %#v", prepared[0])
	}

	prepared = PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true, AllowNVIDIA: true})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("GPU-allocated NVIDIA device was not reopened: %#v", prepared[0])
	}
}

func TestPrepareFDPlansAllowsNVIDIAControlDeviceWithGPUAllocation(t *testing.T) {
	plan := FDPlan{
		FD:          7,
		Kind:        FDKindCharDevice,
		Action:      FDActionForceLocal,
		Path:        "/dev/nvidiactl",
		ReopenPath:  "/dev/nvidiactl",
		DeviceMajor: 195,
		DeviceMinor: 255,
		Reason:      "NVIDIA device descriptor requires an allocated GPU for remote reopening",
		ReasonCode:  FDReasonNVIDIARequiresGPU,
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action == FDActionReopen {
		t.Fatalf("NVIDIA control device reopened without GPU allocation: %#v", prepared[0])
	}
	prepared = PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true, AllowNVIDIA: true})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("GPU-allocated NVIDIA control device was not reopened: %#v", prepared[0])
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
	if plan.ReasonCode != FDReasonUnixSocket {
		t.Fatalf("reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonUnixSocket, plan)
	}
	if plan.SocketFamily != "unix" {
		t.Fatalf("socket family = %q, want unix; plan=%#v", plan.SocketFamily, plan)
	}
}

func TestClassifyInheritedFDsReportsUnixSocketpairReasonCode(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	if _, err := unix.FcntlInt(uintptr(fds[0]), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fds[0])
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", fds[0], plans)
	}
	if plan.Kind != FDKindSocket || plan.SocketFamily != "unix" || plan.ReasonCode != FDReasonUnixSocket {
		t.Fatalf("socketpair plan = %#v", plan)
	}
}

func TestClassifyInheritedFDsReportsUnixPathnameSocketReasonCode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "listener.sock")
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		t.Fatalf("bind unix socket: %v", err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fd)
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", fd, plans)
	}
	if plan.Kind != FDKindSocket || plan.SocketFamily != "unix" || plan.SocketAddress != path || plan.ReasonCode != FDReasonUnixSocket {
		t.Fatalf("pathname unix socket plan = %#v", plan)
	}
}

func TestClassifyInheritedFDsReportsUnixAbstractSocketReasonCode(t *testing.T) {
	name := "\x00octopos-test-" + strconv.Itoa(os.Getpid())
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: name}); err != nil {
		t.Fatalf("bind abstract unix socket: %v", err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, fd)
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", fd, plans)
	}
	if plan.Kind != FDKindSocket || plan.SocketFamily != "unix" || plan.ReasonCode != FDReasonUnixAbstractSocket {
		t.Fatalf("abstract unix socket plan = %#v", plan)
	}
	if !strings.HasPrefix(plan.SocketAddress, "@octopos-test-") {
		t.Fatalf("abstract socket address = %q; plan=%#v", plan.SocketAddress, plan)
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
	if eventPlan.ReasonCode != FDReasonEventFD {
		t.Fatalf("event reason code = %q, want %q; plan=%#v", eventPlan.ReasonCode, FDReasonEventFD, eventPlan)
	}
	timerPlan, ok := findPlan(plans, timerFD)
	if !ok {
		t.Fatalf("timer fd %d missing from plan: %#v", timerFD, plans)
	}
	if timerPlan.Kind != FDKindAnonInode || !strings.Contains(timerPlan.Reason, "timerfd") {
		t.Fatalf("timer plan = %#v", timerPlan)
	}
	if timerPlan.ReasonCode != FDReasonTimerFD {
		t.Fatalf("timer reason code = %q, want %q; plan=%#v", timerPlan.ReasonCode, FDReasonTimerFD, timerPlan)
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
	if plan.ReasonCode != FDReasonSignalFD {
		t.Fatalf("signalfd reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonSignalFD, plan)
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
	if plan.ReasonCode != FDReasonSharedMemoryDeleted && plan.ReasonCode != FDReasonSharedMemory {
		t.Fatalf("memfd reason code = %q, want shared-memory code; plan=%#v", plan.ReasonCode, plan)
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
	if plan.ReasonCode != FDReasonPidFD {
		t.Fatalf("pidfd reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonPidFD, plan)
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
	if plan.Kind != FDKindSocket || !strings.Contains(plan.Reason, "netlink") {
		t.Fatalf("netlink plan = %#v", plan)
	}
	if plan.ReasonCode != FDReasonNetlinkSocket {
		t.Fatalf("netlink reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonNetlinkSocket, plan)
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
	if prepared[0].ReasonCode != FDReasonSharedMemory {
		t.Fatalf("/dev/shm reason code = %q, want %q; plan=%#v", prepared[0].ReasonCode, FDReasonSharedMemory, prepared[0])
	}
}

func TestClassifyInheritedFDsReportsFileLock(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "locked-file"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	clearCloseOnExec(t, file)
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("flock: %v", err)
	}
	defer unix.Flock(int(file.Fd()), unix.LOCK_UN)

	plans, err := ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	plan, ok := findPlan(plans, int(file.Fd()))
	if !ok {
		t.Fatalf("fd %d missing from plan: %#v", file.Fd(), plans)
	}
	if plan.ReasonCode != FDReasonFileLock {
		t.Fatalf("reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonFileLock, plan)
	}
	if len(plan.FileLockTypes) == 0 {
		t.Fatalf("file lock types missing: %#v", plan)
	}
	prepared := PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true})
	if prepared[0].Action == FDActionReopen {
		t.Fatalf("locked fd became reopenable: %#v", prepared[0])
	}
	prepared = PrepareFDPlans([]FDPlan{plan}, FDPlanOptions{AllowReopen: true, AllowFileLocks: true})
	if prepared[0].Action != FDActionReopen {
		t.Fatalf("locked fd was not reopenable with AllowFileLocks: %#v", prepared[0])
	}
}

func TestProcLocksInode(t *testing.T) {
	inode, ok := procLocksInode("08:01:12345")
	if !ok || inode != 12345 {
		t.Fatalf("procLocksInode = %d/%t, want 12345/true", inode, ok)
	}
	if _, ok := procLocksInode("bad"); ok {
		t.Fatal("procLocksInode accepted invalid token")
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
	if plan.ReasonCode != FDReasonDeleted {
		t.Fatalf("reason code = %q, want %q; plan=%#v", plan.ReasonCode, FDReasonDeleted, plan)
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
