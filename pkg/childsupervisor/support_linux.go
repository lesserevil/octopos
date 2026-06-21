//go:build linux

package childsupervisor

import (
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type seccompNotifSizes struct {
	Notif     uint16
	NotifResp uint16
	Data      uint16
}

func CheckSupport() SupportReport {
	report := SupportReport{OS: runtime.GOOS, Arch: runtime.GOARCH, SeccompSyscall: true}

	action := uint32(unix.SECCOMP_RET_USER_NOTIF)
	if errno := seccomp(unix.SECCOMP_GET_ACTION_AVAIL, 0, unsafe.Pointer(&action)); errno != 0 {
		report.UserNotificationActionErr = errno.Error()
	} else {
		report.UserNotificationAction = true
	}

	var sizes seccompNotifSizes
	if errno := seccomp(unix.SECCOMP_GET_NOTIF_SIZES, 0, unsafe.Pointer(&sizes)); errno != 0 {
		report.UserNotificationSizesErr = errno.Error()
	} else {
		report.UserNotificationSizes = true
		report.UserNotificationSize = sizes.Notif
		report.UserNotificationRespSize = sizes.NotifResp
		report.UserNotificationDataSize = sizes.Data
	}

	report.ProductionSupervisorUsable = report.UserNotificationAction && report.UserNotificationSizes
	return report
}

func seccomp(operation int, flags uint, arg unsafe.Pointer) syscall.Errno {
	_, _, errno := unix.RawSyscall(unix.SYS_SECCOMP, uintptr(operation), uintptr(flags), uintptr(arg))
	return errno
}
