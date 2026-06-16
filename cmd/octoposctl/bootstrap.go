package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type bootstrapConfig struct {
	NodeID        string
	WgIP          string
	WgInterface   string
	WgListenPort  int
	GrpcAddr      string
	GrpcPort      int
	EnableGateway bool
	VipIP         string
}

func bootstrapCluster(cfg *bootstrapConfig) error {
	fmt.Println("=== OctopOS Cluster Bootstrap ===")
	fmt.Printf("Bootstrapping cluster as node %s (%s)...\n", cfg.NodeID, cfg.WgIP)

	// 1. Check if octoposd is already running
	if out, _ := exec.Command("pgrep", "octoposd").Output(); len(out) > 0 {
		fmt.Println("octoposd is already running. Skipping bootstrap.")
		return nil
	}

	// 2. Ensure WireGuard tools are installed
	if _, err := exec.LookPath("wg"); err != nil {
		fmt.Println("WireGuard tools not found. Installing...")
		if err := runCmd(exec.Command("sudo", "apt", "install", "-y", "-qq", "wireguard")); err != nil {
			return fmt.Errorf("install wireguard: %w", err)
		}
	}

	// 3. Generate WireGuard keys if needed
	if _, err := os.Stat("/etc/wireguard/private.key"); os.IsNotExist(err) {
		fmt.Print("Generating WireGuard keys... ")
		if err := runCmd(exec.Command("sudo", "sh", "-c",
			"wg genkey | sudo tee /etc/wireguard/private.key | wg pubkey | sudo tee /etc/wireguard/public.key",
		)); err != nil {
			return fmt.Errorf("generate keys: %w", err)
		}
		fmt.Println("OK")
	}

	// 4. Read keys
	privKeyB, err := os.ReadFile("/etc/wireguard/private.key")
	if err != nil {
		// Try reading with sudo
		out, err := exec.Command("sudo", "cat", "/etc/wireguard/private.key").Output()
		if err != nil {
			return fmt.Errorf("read private key: %w", err)
		}
		privKeyB = out
	}
	if cfg.WgListenPort == 0 {
		cfg.WgListenPort = 51820
	}

	// 5. Write WireGuard config
	wgConfig := fmt.Sprintf(`[Interface]
Address = %s/24
PrivateKey = %s
ListenPort = %d
SaveConfig = true
`, cfg.WgIP, strings.TrimSpace(string(privKeyB)), cfg.WgListenPort)

	tmpWgCfg := "/tmp/wg-octopos.conf"
	if err := os.WriteFile(tmpWgCfg, []byte(wgConfig), 0600); err != nil {
		return fmt.Errorf("write wg config: %w", err)
	}
	defer os.Remove(tmpWgCfg)

	fmt.Print("Installing WireGuard config... ")
	if err := runCmd(exec.Command("sudo", "cp", tmpWgCfg, "/etc/wireguard/wg-octopos.conf")); err != nil {
		return fmt.Errorf("copy wg config: %w", err)
	}
	if err := runCmd(exec.Command("sudo", "chmod", "600", "/etc/wireguard/wg-octopos.conf")); err != nil {
		return err
	}
	fmt.Println("OK")

	// 6. Start WireGuard
	fmt.Print("Starting WireGuard... ")
	if err := runCmd(exec.Command("sudo", "wg-quick", "up", "wg-octopos")); err != nil {
		// May already be up
		fmt.Println("(already up or will retry)")
	}
	fmt.Println("OK")

	// 7. Build octoposd if not already built
	if _, err := os.Stat("bin/octoposd"); os.IsNotExist(err) {
		if _, err := os.Stat("/usr/local/bin/octoposd"); os.IsNotExist(err) {
			fmt.Print("Building octoposd... ")
			if err := runCmd(exec.Command("go", "build", "-o", "bin/octoposd", "./cmd/octoposd")); err != nil {
				return fmt.Errorf("build octoposd: %w", err)
			}
			fmt.Println("OK")
			if err := runCmd(exec.Command("sudo", "cp", "bin/octoposd", "/usr/local/bin/octoposd")); err != nil {
				return err
			}
		}
	} else {
		if err := runCmd(exec.Command("sudo", "cp", "bin/octoposd", "/usr/local/bin/octoposd")); err != nil {
			return err
		}
	}

	// 8. Install systemd service
	svcContent := fmt.Sprintf(`[Unit]
Description=OctopOS Cluster Daemon
After=network.target wg-quick@wg-octopos.service
Wants=wg-quick@wg-octopos.service

[Service]
ExecStart=/usr/local/bin/octoposd --node-id %s --grpc-addr %s --wg-interface wg-octopos
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
`, cfg.NodeID, cfg.GrpcAddr)

	tmpSvc := "/tmp/octoposd.service"
	if err := os.WriteFile(tmpSvc, []byte(svcContent), 0644); err != nil {
		return fmt.Errorf("write service file: %w", err)
	}
	defer os.Remove(tmpSvc)

	fmt.Print("Installing systemd service... ")
	if err := runCmd(exec.Command("sudo", "cp", tmpSvc, "/etc/systemd/system/octoposd.service")); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "daemon-reload")); err != nil {
		return err
	}
	fmt.Println("OK")

	// 9. Start octoposd
	fmt.Print("Starting octoposd... ")
	if err := runCmd(exec.Command("sudo", "systemctl", "enable", "octoposd")); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "systemctl", "start", "octoposd")); err != nil {
		return err
	}
	fmt.Println("OK")

	// 10. Assign VIP to WireGuard interface
	if cfg.VipIP != "" {
		fmt.Printf("Assigning VIP %s to %s... ", cfg.VipIP, cfg.WgInterface)
		vipCmd := exec.Command("sudo", "ip", "addr", "add", cfg.VipIP+"/32", "dev", cfg.WgInterface)
		if err := runCmd(vipCmd); err != nil {
			// May already exist
			fmt.Println("(already assigned or will retry)")
		} else {
			fmt.Println("OK")
		}
	}

	// 11. Deploy VIP Gateway (octopos-gw)
	if cfg.EnableGateway {
		fmt.Print("Building and deploying octopos-gw... ")
		if err := runCmd(exec.Command("go", "build", "-o", "bin/octopos-gw", "./cmd/octopos-gw")); err != nil {
			return fmt.Errorf("build octopos-gw: %w", err)
		}
		if err := runCmd(exec.Command("sudo", "cp", "bin/octopos-gw", "/usr/local/bin/octopos-gw")); err != nil {
			return err
		}
		fmt.Println("OK")

		fmt.Print("Installing octopos-gw systemd service... ")
		gwSvcContent := fmt.Sprintf(`[Unit]
Description=OctopOS Cluster Gateway (VIP SSH)
After=network.target wg-quick@%s.service octoposd.service
Wants=wg-quick@%s.service octoposd.service

[Service]
Type=simple
ExecStart=/usr/local/bin/octopos-gw --vip %s:22 --grpc-addr 127.0.0.1:%d --node-id %s --mount-base /tmp/octopos --host-key /etc/ssh/ssh_host_ed25519_key
Restart=always
RestartSec=5
User=root
AmbientCapabilities=CAP_SYS_ADMIN CAP_SYS_RESOURCE CAP_DAC_OVERRIDE
CapabilityBoundingSet=CAP_SYS_ADMIN CAP_SYS_RESOURCE CAP_DAC_OVERRIDE

[Install]
WantedBy=multi-user.target
`, cfg.WgInterface, cfg.WgInterface, cfg.VipIP, cfg.GrpcPort, cfg.NodeID)

		tmpGwSvc := "/tmp/octopos-gw.service"
		if err := os.WriteFile(tmpGwSvc, []byte(gwSvcContent), 0644); err != nil {
			return fmt.Errorf("write gw service file: %w", err)
		}
		defer os.Remove(tmpGwSvc)

		if err := runCmd(exec.Command("sudo", "cp", tmpGwSvc, "/etc/systemd/system/octopos-gw.service")); err != nil {
			return err
		}
		if err := runCmd(exec.Command("sudo", "systemctl", "daemon-reload")); err != nil {
			return err
		}
		fmt.Println("OK")

		fmt.Print("Starting octopos-gw... ")
		if err := runCmd(exec.Command("sudo", "systemctl", "enable", "octopos-gw")); err != nil {
			return err
		}
		if err := runCmd(exec.Command("sudo", "systemctl", "start", "octopos-gw")); err != nil {
			return err
		}
		fmt.Println("OK")
	}

	fmt.Println("\n=== Bootstrap Complete ===")
	fmt.Printf("Cluster initialized with node %s (%s)\n", cfg.NodeID, cfg.WgIP)
	if cfg.EnableGateway && cfg.VipIP != "" {
		fmt.Printf("VIP Gateway available at: ssh user@%s\n", cfg.VipIP)
	}
	fmt.Println("")
	fmt.Println("To add more nodes, run on this node:")
	fmt.Printf("  octoposctl --addr %s node add <node-id> --address <ssh-addr> --wg-ip <wg-ip>\n", cfg.GrpcAddr)
	fmt.Println("")
	fmt.Println("To check cluster status:")
	fmt.Printf("  octoposctl --addr %s cluster status\n", cfg.GrpcAddr)

	return nil
}

func runCmd(cmd *exec.Cmd) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
