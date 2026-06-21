package main

import (
	"fmt"
	"os"
	"os/exec"
)

const defaultClusterFSOptions = "allow_other,dev,suid"

func clusterFSUnitContent(mountPoint, metaURL, options string, requireObjectStoreProxy bool) string {
	if options == "" {
		options = defaultClusterFSOptions
	}
	after := "network-online.target docker.service"
	wants := after
	if requireObjectStoreProxy {
		after += " octopos-objectstore-proxy.service"
		wants += " octopos-objectstore-proxy.service"
	}
	return fmt.Sprintf(`[Unit]
Description=OctopOS JuiceFS cluster filesystem
After=%s
Wants=%s

[Service]
Type=simple
ExecStartPre=/bin/mkdir -p %s
ExecStart=/usr/local/bin/juicefs mount --cache-large-write -o %s %s %s
ExecStartPost=/bin/sh -c 'for i in $(seq 1 120); do mountpoint -q %s && exit 0; sleep 1; done; exit 1'
ExecStop=/bin/umount %s
Restart=on-failure
RestartSec=10
TimeoutStartSec=120
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
`, after, wants, mountPoint, options, metaURL, mountPoint, mountPoint, mountPoint)
}

func localSystemdUnitExists(unit string) bool {
	return exec.Command("systemctl", "cat", unit).Run() == nil
}

func installLocalClusterFSService(mountPoint, metaURL, options string, requireObjectStoreProxy bool) error {
	if _, err := exec.LookPath("juicefs"); err != nil {
		return fmt.Errorf("juicefs is required to install octopos-clusterfs.service: %w", err)
	}
	tmpSvc := "/tmp/octopos-clusterfs.service"
	if err := os.WriteFile(tmpSvc, []byte(clusterFSUnitContent(mountPoint, metaURL, options, requireObjectStoreProxy)), 0644); err != nil {
		return fmt.Errorf("write clusterfs service file: %w", err)
	}
	defer os.Remove(tmpSvc)
	if err := runCmd(exec.Command("sudo", "cp", tmpSvc, "/etc/systemd/system/octopos-clusterfs.service")); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "daemon-reload")); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "enable", "octopos-clusterfs")); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "start", "octopos-clusterfs")); err != nil {
		return err
	}
	return nil
}
