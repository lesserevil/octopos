package childsupervisor

import (
	"fmt"
	"strings"
)

type SupportReport struct {
	OS                        string
	Arch                      string
	SeccompSyscall            bool
	UserNotificationAction    bool
	UserNotificationSizes     bool
	UserNotificationSize      uint16
	UserNotificationRespSize  uint16
	UserNotificationDataSize  uint16
	UserNotificationActionErr string
	UserNotificationSizesErr  string
	SeccompAuditUsable        bool
	// Deprecated: use SeccompAuditUsable or AuditUsable.
	ProductionSupervisorUsable bool
}

func (r SupportReport) AuditUsable() bool {
	return r.SeccompAuditUsable || r.ProductionSupervisorUsable
}

func (r SupportReport) Lines() []string {
	lines := []string{
		fmt.Sprintf("os=%s", r.OS),
		fmt.Sprintf("arch=%s", r.Arch),
		fmt.Sprintf("seccomp_syscall=%t", r.SeccompSyscall),
		fmt.Sprintf("user_notification_action=%t", r.UserNotificationAction),
		fmt.Sprintf("user_notification_sizes=%t", r.UserNotificationSizes),
	}
	if r.UserNotificationActionErr != "" {
		lines = append(lines, "user_notification_action_error="+r.UserNotificationActionErr)
	}
	if r.UserNotificationSizesErr != "" {
		lines = append(lines, "user_notification_sizes_error="+r.UserNotificationSizesErr)
	}
	if r.UserNotificationSizes {
		lines = append(lines,
			fmt.Sprintf("seccomp_notif_size=%d", r.UserNotificationSize),
			fmt.Sprintf("seccomp_notif_resp_size=%d", r.UserNotificationRespSize),
			fmt.Sprintf("seccomp_data_size=%d", r.UserNotificationDataSize),
		)
	}
	lines = append(lines, fmt.Sprintf("seccomp_audit_usable=%t", r.AuditUsable()))
	return lines
}

func (r SupportReport) String() string {
	return strings.Join(r.Lines(), "\n")
}
