package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultObjectStoreProxyListen  = "127.0.0.1:19000"
	defaultObjectStoreProxyTargets = "10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000"
)

type objectStoreTarget struct {
	Address string
}

func normalizeObjectStoreListen(listen string) (string, error) {
	if strings.TrimSpace(listen) == "" {
		listen = defaultObjectStoreProxyListen
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("invalid object-store proxy listen address %q: %w", listen, err)
	}
	if host == "" {
		return "", fmt.Errorf("invalid object-store proxy listen address %q: missing host", listen)
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", fmt.Errorf("invalid object-store proxy listen address %q: invalid port", listen)
	}
	return listen, nil
}

func parseObjectStoreTargets(raw string) ([]objectStoreTarget, error) {
	if strings.TrimSpace(raw) == "" {
		raw = defaultObjectStoreProxyTargets
	}
	parts := strings.Split(raw, ",")
	targets := make([]objectStoreTarget, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid object-store target %q: %w", addr, err)
		}
		if host == "" {
			return nil, fmt.Errorf("invalid object-store target %q: missing host", addr)
		}
		if _, err := strconv.Atoi(port); err != nil {
			return nil, fmt.Errorf("invalid object-store target %q: invalid port", addr)
		}
		targets = append(targets, objectStoreTarget{Address: addr})
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no object-store proxy targets configured")
	}
	return targets, nil
}

func objectStoreProxyArgs(listen, rawTargets string) ([]string, error) {
	listen, err := normalizeObjectStoreListen(listen)
	if err != nil {
		return nil, err
	}
	targets, err := parseObjectStoreTargets(rawTargets)
	if err != nil {
		return nil, err
	}
	targetAddrs := make([]string, 0, len(targets))
	for _, target := range targets {
		targetAddrs = append(targetAddrs, target.Address)
	}
	return []string{
		"--listen", listen,
		"--targets", strings.Join(targetAddrs, ","),
		"--health-path", "/minio/health/cluster",
	}, nil
}

func objectStoreProxyUnitContent(listen, rawTargets string) (string, error) {
	args, err := objectStoreProxyArgs(listen, rawTargets)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`[Unit]
Description=OctopOS object store proxy
After=network-online.target docker.service
Wants=network-online.target docker.service

[Service]
Type=simple
ExecStart=/usr/local/bin/octopos-objectstore-proxy %s
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
`, strings.Join(args, " ")), nil
}

func installLocalObjectStoreProxy(listen, targets string) error {
	if err := runCmd(exec.Command("go", "build", "-o", filepath.Join("bin", "octopos-objectstore-proxy"), "./cmd/octopos-objectstore-proxy")); err != nil {
		return fmt.Errorf("build octopos-objectstore-proxy: %w", err)
	}
	if err := runCmd(exec.Command("sudo", "cp", filepath.Join("bin", "octopos-objectstore-proxy"), "/usr/local/bin/octopos-objectstore-proxy")); err != nil {
		return err
	}
	unit, err := objectStoreProxyUnitContent(listen, targets)
	if err != nil {
		return err
	}
	tmpSvc := "/tmp/octopos-objectstore-proxy.service"
	if err := os.WriteFile(tmpSvc, []byte(unit), 0644); err != nil {
		return fmt.Errorf("write object-store proxy service: %w", err)
	}
	defer os.Remove(tmpSvc)
	installCmd := exec.Command("sudo", "install", "-m", "0644", tmpSvc, "/etc/systemd/system/octopos-objectstore-proxy.service")
	if err := runCmd(installCmd); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "daemon-reload")); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "enable", "octopos-objectstore-proxy")); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "restart", "octopos-objectstore-proxy")); err != nil {
		return err
	}
	return nil
}
