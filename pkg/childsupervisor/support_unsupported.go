//go:build !linux

package childsupervisor

import "runtime"

func CheckSupport() SupportReport {
	return SupportReport{
		OS:                         runtime.GOOS,
		Arch:                       runtime.GOARCH,
		SeccompSyscall:             false,
		UserNotificationActionErr:  "seccomp user notification requires linux",
		UserNotificationSizesErr:   "seccomp user notification requires linux",
		SeccompAuditUsable:         false,
		ProductionSupervisorUsable: false,
	}
}
