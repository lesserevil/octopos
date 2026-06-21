package childsupervisor

import (
	"strings"
	"testing"
)

func TestSupportReportString(t *testing.T) {
	report := SupportReport{
		OS:                         "linux",
		Arch:                       "amd64",
		SeccompSyscall:             true,
		UserNotificationAction:     true,
		UserNotificationSizes:      true,
		UserNotificationSize:       80,
		UserNotificationRespSize:   24,
		UserNotificationDataSize:   64,
		ProductionSupervisorUsable: true,
	}
	out := report.String()
	for _, want := range []string{
		"os=linux",
		"arch=amd64",
		"seccomp_syscall=true",
		"user_notification_action=true",
		"user_notification_sizes=true",
		"seccomp_notif_size=80",
		"production_supervisor_usable=true",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("support report missing %q:\n%s", want, out)
		}
	}
}
