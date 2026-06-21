package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/octopos/octopos/pkg/clusterconfig"
	"github.com/octopos/octopos/pkg/remotechild"
	"github.com/octopos/octopos/pkg/ssi"
)

func buildClusterConfig(nodeID, grpcAddr, wgInterface, clusterRoot, ssiRootFS string, requireSSI bool, peers string, execDefaults clusterconfig.ExecDefaults) clusterconfig.File {
	ssiCfg := ssi.Config{ClusterRoot: clusterRoot, RootFS: ssiRootFS, Required: requireSSI}.WithDefaults()
	return clusterconfig.File{
		NodeID:        nodeID,
		GRPCAddr:      grpcAddr,
		WGInterface:   wgInterface,
		JuiceFSMount:  ssiCfg.ClusterRoot,
		SSIRootFS:     ssiCfg.RootFS,
		SSIMountBase:  ssi.DefaultMountBase,
		SSIExecutor:   ssi.DefaultExecutor,
		RequireSSI:    requireSSI,
		ChildSocket:   remotechild.DefaultSocketPath,
		ChildState:    "/var/lib/octopos/remote-children.json",
		ChildLease:    "2m",
		ChildTokenTTL: "24h",
		Peers:         peers,
		VFIOEnabled:   true,
		ExecDefaults:  execDefaults.WithDefaults(),
	}
}

func writeTempClusterConfig(cfg clusterconfig.File) (string, error) {
	data, err := clusterconfig.MarshalFile(cfg)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp("", "octoposd-*.yaml")
	if err != nil {
		return "", fmt.Errorf("create temporary cluster config: %w", err)
	}
	path := file.Name()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("write temporary cluster config: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close temporary cluster config: %w", err)
	}
	return path, nil
}

func installLocalClusterConfig(cfg clusterconfig.File) error {
	tmp, err := writeTempClusterConfig(cfg)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := runCmd(exec.Command("sudo", "install", "-d", filepath.Dir(clusterconfig.DefaultPath))); err != nil {
		return err
	}
	if err := runCmd(exec.Command("sudo", "install", "-m", "0644", tmp, clusterconfig.DefaultPath)); err != nil {
		return err
	}
	return nil
}

func (p *provisionConfig) installRemoteClusterConfig(cfg clusterconfig.File) error {
	tmp, err := writeTempClusterConfig(cfg)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := p.run("scp cluster config", p.scpCmd(tmp, "/tmp/octoposd.yaml")); err != nil {
		return err
	}
	installCmd := fmt.Sprintf("sudo install -d %s && sudo install -m 0644 /tmp/octoposd.yaml %s",
		shellQuote(filepath.Dir(clusterconfig.DefaultPath)),
		shellQuote(clusterconfig.DefaultPath),
	)
	return p.run("install cluster config", p.sshCmd(installCmd))
}
