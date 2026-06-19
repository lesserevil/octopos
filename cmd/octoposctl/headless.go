package main

import (
	"os/exec"
	"strings"
)

const headlessSystemdScript = `
set -eu
if ! command -v systemctl >/dev/null 2>&1; then
	exit 0
fi

systemctl set-default multi-user.target
systemctl disable graphical.target 2>/dev/null || true
systemctl disable display-manager.service 2>/dev/null || true

for svc in gdm3 gdm lightdm sddm xdm lxdm; do
	systemctl disable "$svc.service" 2>/dev/null || true
	systemctl stop --no-block "$svc.service" 2>/dev/null || true
done

systemctl stop --no-block display-manager.service graphical.target 2>/dev/null || true
`

func localHeadlessSystemdCmd() *exec.Cmd {
	return exec.Command("sudo", "sh", "-c", headlessSystemdScript)
}

func remoteHeadlessSystemdCommand() string {
	return "sudo sh -c " + shellSingleQuote(headlessSystemdScript)
}

func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}
