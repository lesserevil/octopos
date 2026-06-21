package remotechild

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type FDAction string

const (
	FDActionProxyStream FDAction = "proxy_stream"
	FDActionClose       FDAction = "close"
	FDActionReopen      FDAction = "reopen"
	FDActionForceLocal  FDAction = "force_local"
)

type FDKind string

const (
	FDKindStdio       FDKind = "stdio"
	FDKindCloseOnExec FDKind = "close_on_exec"
	FDKindRegular     FDKind = "regular"
	FDKindDirectory   FDKind = "directory"
	FDKindPipe        FDKind = "pipe"
	FDKindSocket      FDKind = "socket"
	FDKindCharDevice  FDKind = "char_device"
	FDKindBlockDevice FDKind = "block_device"
	FDKindAnonInode   FDKind = "anon_inode"
	FDKindUnknown     FDKind = "unknown"
)

type FDPlan struct {
	FD          int
	Kind        FDKind
	Action      FDAction
	Path        string
	ReopenPath  string
	DeviceMajor uint32
	DeviceMinor uint32
	PipeID      string
	SocketID    string
	OpenFlags   int
	Offset      int64
	Deleted     bool
	Reason      string
}

type FDPlanOptions struct {
	AllowReopen bool
}

type ReopenFD struct {
	FD     int    `json:"fd"`
	Path   string `json:"path"`
	Flags  int    `json:"flags"`
	Offset int64  `json:"offset,omitempty"`
	Kind   FDKind `json:"kind"`
}

func ClassifyInheritedFDs(pid int) ([]FDPlan, error) {
	if pid == os.Getpid() {
		return classifyCurrentProcessFDs()
	}
	fdDir := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", fdDir, err)
	}

	plans := make([]FDPlan, 0, len(entries))
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		target, err := os.Readlink(filepath.Join(fdDir, entry.Name()))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read fd %d: %w", fd, err)
		}
		flags := procFDFlags(pid, fd)
		openFlags, offset := procFDOpenInfo(pid, fd)
		plans = append(plans, classifyOpenFD(fd, target, flags, openFlags, offset, detectKindFromTarget(target), false))
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].FD < plans[j].FD })
	return plans, nil
}

func classifyCurrentProcessFDs() ([]FDPlan, error) {
	maxFD := uint64(1024)
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err == nil && limit.Cur > 0 {
		maxFD = limit.Cur
	}
	if maxFD > 65536 {
		maxFD = 65536
	}

	plans := make([]FDPlan, 0, 16)
	for fd := 0; uint64(fd) < maxFD; fd++ {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil {
			continue
		}
		target := currentFDTarget(fd)
		openFlags, offset := currentFDOpenInfo(fd)
		plans = append(plans, classifyOpenFD(fd, target, flags, openFlags, offset, detectKindFromFD(fd, target), true))
	}
	return plans, nil
}

func PrepareFDPlans(plans []FDPlan, opts FDPlanOptions) []FDPlan {
	out := make([]FDPlan, 0, len(plans))
	for _, plan := range plans {
		if opts.AllowReopen && reopenableFDPlan(plan) {
			plan.Action = FDActionReopen
			plan.Reason = "descriptor will be reopened in the remote SSI namespace"
		}
		out = append(out, plan)
	}
	return out
}

func ReopenFDs(plans []FDPlan) []ReopenFD {
	out := make([]ReopenFD, 0)
	for _, plan := range plans {
		if plan.Action != FDActionReopen {
			continue
		}
		out = append(out, ReopenFD{
			FD:     plan.FD,
			Path:   plan.ReopenPath,
			Flags:  sanitizeReopenFlags(plan.OpenFlags),
			Offset: plan.Offset,
			Kind:   plan.Kind,
		})
	}
	return out
}

func EncodeReopenFDs(fds []ReopenFD) (string, error) {
	if len(fds) == 0 {
		return "", nil
	}
	data, err := json.Marshal(fds)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func DecodeReopenFDs(raw string) ([]ReopenFD, error) {
	if raw == "" {
		return nil, nil
	}
	var fds []ReopenFD
	if err := json.Unmarshal([]byte(raw), &fds); err != nil {
		return nil, err
	}
	return fds, nil
}

func UnsupportedFDs(plans []FDPlan) []FDPlan {
	var out []FDPlan
	for _, plan := range plans {
		if plan.Action == FDActionForceLocal {
			out = append(out, plan)
		}
	}
	return out
}

func FormatUnsupportedFDs(plans []FDPlan) string {
	if len(plans) == 0 {
		return ""
	}
	parts := make([]string, 0, len(plans))
	for _, plan := range plans {
		parts = append(parts, fmt.Sprintf("fd %d %s %s%s: %s", plan.FD, plan.Kind, plan.Path, fdPlanDetailSuffix(plan), plan.Reason))
	}
	return strings.Join(parts, "; ")
}

func classifyOpenFD(fd int, target string, flags int, openFlags int, offset int64, kind FDKind, currentProcess bool) FDPlan {
	if fd >= 0 && fd <= 2 {
		plan := FDPlan{
			FD:        fd,
			Kind:      FDKindStdio,
			Action:    FDActionProxyStream,
			Path:      target,
			OpenFlags: openFlags,
			Offset:    offset,
			Reason:    "stdio is proxied",
		}
		enrichFDPlan(&plan, fd, target, currentProcess)
		return plan
	}
	if flags&unix.FD_CLOEXEC != 0 {
		plan := FDPlan{
			FD:        fd,
			Kind:      FDKindCloseOnExec,
			Action:    FDActionClose,
			Path:      target,
			OpenFlags: openFlags,
			Offset:    offset,
			Reason:    "descriptor is close-on-exec",
		}
		enrichFDPlan(&plan, fd, target, currentProcess)
		return plan
	}
	plan := FDPlan{
		FD:        fd,
		Kind:      kind,
		Action:    FDActionForceLocal,
		Path:      target,
		OpenFlags: openFlags,
		Offset:    offset,
	}
	enrichFDPlan(&plan, fd, target, currentProcess)
	plan.Reason = forceLocalReason(plan)
	return plan
}

func reopenableFDPlan(plan FDPlan) bool {
	if plan.FD < 3 || plan.Deleted || plan.ReopenPath == "" || !filepath.IsAbs(plan.ReopenPath) {
		return false
	}
	switch plan.Kind {
	case FDKindRegular:
		return !strings.HasPrefix(plan.ReopenPath, "/dev/shm/") &&
			!strings.HasPrefix(plan.ReopenPath, "/proc/") &&
			!strings.HasPrefix(plan.ReopenPath, "/sys/") &&
			!strings.HasPrefix(plan.ReopenPath, "/run/octopos/")
	case FDKindCharDevice:
		return knownReopenableCharDevice(plan.ReopenPath)
	default:
		return false
	}
}

func sanitizeReopenFlags(flags int) int {
	out := flags & unix.O_ACCMODE
	for _, flag := range []int{unix.O_APPEND, unix.O_NONBLOCK, unix.O_SYNC, unix.O_DSYNC, unix.O_RSYNC} {
		if flags&flag != 0 {
			out |= flag
		}
	}
	return out
}

func enrichFDPlan(plan *FDPlan, fd int, target string, currentProcess bool) {
	if strings.Contains(target, " (deleted)") {
		plan.Deleted = true
	}
	if id := bracketedID(target, "pipe:"); id != "" {
		plan.PipeID = id
	}
	if id := bracketedID(target, "socket:"); id != "" {
		plan.SocketID = id
	}
	if currentProcess {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err == nil {
			switch st.Mode & unix.S_IFMT {
			case unix.S_IFCHR, unix.S_IFBLK:
				plan.DeviceMajor = uint32(unix.Major(uint64(st.Rdev)))
				plan.DeviceMinor = uint32(unix.Minor(uint64(st.Rdev)))
			}
		}
	}
	if plan.Kind == FDKindRegular || plan.Kind == FDKindCharDevice || plan.Kind == FDKindBlockDevice {
		if !plan.Deleted && filepath.IsAbs(target) {
			plan.ReopenPath = target
		}
	}
}

func forceLocalReason(plan FDPlan) string {
	if plan.Deleted {
		if strings.Contains(plan.Path, "/dev/shm/") || strings.HasPrefix(plan.Path, "/memfd:") || strings.HasPrefix(plan.Path, "memfd:") {
			return "shared-memory descriptor is deleted and cannot be represented remotely"
		}
		return "deleted descriptor cannot be reopened remotely"
	}
	switch plan.Kind {
	case FDKindRegular:
		if strings.HasPrefix(plan.Path, "/dev/shm/") || strings.HasPrefix(plan.Path, "/memfd:") || strings.HasPrefix(plan.Path, "memfd:") {
			return "shared-memory descriptor is local kernel state and is not distributed"
		}
		if plan.ReopenPath != "" {
			return "regular file descriptor requires remote fd recreation, which is not enabled yet"
		}
		return "regular file descriptor cannot be mapped to a reopen path"
	case FDKindDirectory:
		return "directory descriptor requires remote fd recreation, which is not enabled yet"
	case FDKindPipe:
		return "anonymous pipe requires coordinated pipe proxying"
	case FDKindSocket:
		if plan.SocketID != "" {
			return "socket descriptor requires local kernel peer state"
		}
		return "socket descriptor cannot be represented remotely"
	case FDKindAnonInode:
		switch {
		case strings.Contains(plan.Path, "eventfd"):
			return "eventfd descriptor is local kernel state and is not distributed"
		case strings.Contains(plan.Path, "signalfd"):
			return "signalfd descriptor is local kernel state and is not distributed"
		case strings.Contains(plan.Path, "timerfd"):
			return "timerfd descriptor is local kernel state and is not distributed"
		case strings.Contains(plan.Path, "pidfd"):
			return "pidfd descriptor is local kernel state and is not distributed"
		default:
			return "anonymous inode descriptor is local kernel state and is not distributed"
		}
	case FDKindCharDevice:
		if knownReopenableCharDevice(plan.ReopenPath) {
			return fmt.Sprintf("%s can be reopened remotely, but remote fd recreation is not enabled yet", plan.ReopenPath)
		}
		if plan.ReopenPath != "" {
			return "character device descriptor requires an explicit device allowlist"
		}
		return "character device descriptor cannot be mapped to a reopen path"
	case FDKindBlockDevice:
		return "block device descriptor requires an explicit device allowlist"
	default:
		return fmt.Sprintf("inherited %s descriptor would not be represented remotely", plan.Kind)
	}
}

func knownReopenableCharDevice(path string) bool {
	switch path {
	case "/dev/full", "/dev/null", "/dev/zero", "/dev/random", "/dev/urandom":
		return true
	default:
		return false
	}
}

func bracketedID(target string, prefix string) string {
	if !strings.HasPrefix(target, prefix+"[") || !strings.HasSuffix(target, "]") {
		return ""
	}
	return strings.TrimSuffix(strings.TrimPrefix(target, prefix+"["), "]")
}

func fdPlanDetailSuffix(plan FDPlan) string {
	var parts []string
	if plan.ReopenPath != "" {
		parts = append(parts, "reopen="+plan.ReopenPath)
	}
	if plan.DeviceMajor != 0 || plan.DeviceMinor != 0 {
		parts = append(parts, fmt.Sprintf("dev=%d:%d", plan.DeviceMajor, plan.DeviceMinor))
	}
	if plan.PipeID != "" {
		parts = append(parts, "pipe="+plan.PipeID)
	}
	if plan.SocketID != "" {
		parts = append(parts, "socket="+plan.SocketID)
	}
	if plan.Deleted {
		parts = append(parts, "deleted")
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ",") + ")"
}

func procFDFlags(pid int, fd int) int {
	flags, _ := procFDInfo(pid, fd)
	if flags&unix.O_CLOEXEC != 0 {
		return unix.FD_CLOEXEC
	}
	return 0
}

func procFDOpenInfo(pid int, fd int) (int, int64) {
	flags, pos := procFDInfo(pid, fd)
	return flags, pos
}

func procFDInfo(pid int, fd int) (int, int64) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "fdinfo", strconv.Itoa(fd)))
	if err != nil {
		return 0, 0
	}
	var flags int
	var pos int64
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "flags":
			raw, err := strconv.ParseUint(strings.TrimSpace(value), 0, 64)
			if err == nil {
				flags = int(raw)
			}
		case "pos":
			raw, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err == nil {
				pos = raw
			}
		}
	}
	return flags, pos
}

func currentFDOpenInfo(fd int) (int, int64) {
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		flags = 0
	}
	offset, err := unix.Seek(fd, 0, unix.SEEK_CUR)
	if err != nil {
		offset = 0
	}
	return flags, offset
}

func currentFDTarget(fd int) string {
	name := strconv.Itoa(fd)
	if dir := os.Getenv(EnvHostFDDir); dir != "" {
		if target, err := os.Readlink(filepath.Join(dir, name)); err == nil && target != "" {
			return target
		}
	}
	if target, err := os.Readlink(filepath.Join("/proc/self/fd", name)); err == nil && target != "" {
		return target
	}
	return fmt.Sprintf("fd:%d", fd)
}

func detectKindFromFD(fd int, target string) FDKind {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err == nil {
		return detectKindFromMode(st.Mode, target)
	}
	return detectKindFromTarget(target)
}

func detectKindFromMode(mode uint32, target string) FDKind {
	switch mode & unix.S_IFMT {
	case unix.S_IFREG:
		return FDKindRegular
	case unix.S_IFDIR:
		return FDKindDirectory
	case unix.S_IFIFO:
		return FDKindPipe
	case unix.S_IFSOCK:
		return FDKindSocket
	case unix.S_IFCHR:
		return FDKindCharDevice
	case unix.S_IFBLK:
		return FDKindBlockDevice
	}
	kind := detectKindFromTarget(target)
	if kind != FDKindUnknown {
		return kind
	}
	return FDKindUnknown
}

func detectKindFromTarget(target string) FDKind {
	switch {
	case strings.HasPrefix(target, "pipe:["):
		return FDKindPipe
	case strings.HasPrefix(target, "socket:["):
		return FDKindSocket
	case strings.HasPrefix(target, "anon_inode:"):
		return FDKindAnonInode
	case strings.Contains(target, " (deleted)"):
		if strings.HasPrefix(target, "anon_inode:") {
			return FDKindAnonInode
		}
	}
	info, err := os.Stat(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return FDKindUnknown
		}
		return FDKindUnknown
	}
	mode := info.Mode()
	switch {
	case mode.IsRegular():
		return FDKindRegular
	case mode.IsDir():
		return FDKindDirectory
	case mode&os.ModeCharDevice != 0:
		return FDKindCharDevice
	case mode&os.ModeDevice != 0:
		return FDKindBlockDevice
	case mode&os.ModeSocket != 0:
		return FDKindSocket
	case mode&os.ModeNamedPipe != 0:
		return FDKindPipe
	}
	return FDKindUnknown
}
