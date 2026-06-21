package clusterconfig

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const DefaultPath = "/etc/octopos/octoposd.yaml"

type ExecDefaults struct {
	CPUCores int `yaml:"cpu_cores"`
	MemoryGB int `yaml:"memory_gb"`
}

type File struct {
	NodeID             string       `yaml:"node_id,omitempty"`
	GRPCAddr           string       `yaml:"grpc_addr,omitempty"`
	WGInterface        string       `yaml:"wg_interface,omitempty"`
	JuiceFSMount       string       `yaml:"juicefs_mount,omitempty"`
	SSIRootFS          string       `yaml:"ssi_rootfs,omitempty"`
	SSIMountBase       string       `yaml:"ssi_mount_base,omitempty"`
	SSIExecutor        string       `yaml:"ssi_executor,omitempty"`
	RequireSSI         bool         `yaml:"require_ssi"`
	ChildSocket        string       `yaml:"child_socket,omitempty"`
	ChildState         string       `yaml:"remote_child_state,omitempty"`
	ChildLease         string       `yaml:"remote_child_lease_timeout,omitempty"`
	ChildTokenTTL      string       `yaml:"remote_child_token_ttl,omitempty"`
	Peers              string       `yaml:"peers,omitempty"`
	VFIOEnabled        bool         `yaml:"vfio_enabled"`
	VFIOAllowedGroups  []int        `yaml:"vfio_allowed_groups,omitempty"`
	VFIODeniedGroups   []int        `yaml:"vfio_denied_groups,omitempty"`
	VFIOAllowedClasses []string     `yaml:"vfio_allowed_classes,omitempty"`
	VFIOAllowedVendors []string     `yaml:"vfio_allowed_vendors,omitempty"`
	VFIODriverRebind   bool         `yaml:"vfio_driver_rebind"`
	ExecDefaults       ExecDefaults `yaml:"exec_defaults"`
}

func DefaultExecDefaults() ExecDefaults {
	return ExecDefaults{
		CPUCores: 1,
		MemoryGB: 1,
	}
}

func (d ExecDefaults) WithDefaults() ExecDefaults {
	defaults := DefaultExecDefaults()
	if d.CPUCores <= 0 {
		d.CPUCores = defaults.CPUCores
	}
	if d.MemoryGB <= 0 {
		d.MemoryGB = defaults.MemoryGB
	}
	return d
}

func LoadFile(path string) (File, error) {
	if path == "" {
		path = DefaultPath
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return File{ExecDefaults: DefaultExecDefaults()}, nil
	}
	if err != nil {
		return File{}, fmt.Errorf("read cluster config %s: %w", path, err)
	}
	var cfg File
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return File{}, fmt.Errorf("parse cluster config %s: %w", path, err)
	}
	cfg.ExecDefaults = cfg.ExecDefaults.WithDefaults()
	return cfg, nil
}

func LoadExecDefaults(path string) (ExecDefaults, error) {
	cfg, err := LoadFile(path)
	if err != nil {
		return ExecDefaults{}, err
	}
	return cfg.ExecDefaults.WithDefaults(), nil
}

func MarshalFile(cfg File) ([]byte, error) {
	cfg.ExecDefaults = cfg.ExecDefaults.WithDefaults()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal cluster config: %w", err)
	}
	return data, nil
}
