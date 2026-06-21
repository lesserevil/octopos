//go:build linux

package childsupervisor

import "testing"

func TestExecNotificationFilterTrapsExecAndAllowsOtherSyscalls(t *testing.T) {
	filters := execNotificationFilter()
	if len(filters) != 5 {
		t.Fatalf("filter length = %d, want 5", len(filters))
	}
	if filters[1].K == filters[2].K {
		t.Fatalf("exec syscall filters are identical: %#v", filters)
	}
	if filters[3].K == filters[4].K {
		t.Fatalf("trap and allow returns are identical: %#v", filters)
	}
}
