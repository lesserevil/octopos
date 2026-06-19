package ssi

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultClusterRoot = "/cluster"
	DefaultRootFS      = DefaultClusterRoot
	DefaultHostname    = "octopos"
	DefaultMountBase   = "/run/octopos/ssi"
	DefaultExecutor    = "/usr/local/bin/octopos-exec"

	LabelReady       = "octopos.io/ssi-ready"
	LabelClusterRoot = "octopos.io/ssi-root"
	LabelRootFS      = "octopos.io/ssi-rootfs"
)

type Config struct {
	ClusterRoot  string
	RootFS       string
	MountBase    string
	Executor     string
	RequireMount bool
	Required     bool
}

func (c Config) WithDefaults() Config {
	if c.ClusterRoot == "" {
		c.ClusterRoot = DefaultClusterRoot
	}
	if c.RootFS == "" {
		c.RootFS = c.ClusterRoot
	}
	if c.MountBase == "" {
		c.MountBase = DefaultMountBase
	}
	if c.Executor == "" {
		c.Executor = DefaultExecutor
	}
	return c
}

func (c Config) Labels() map[string]string {
	cfg := c.WithDefaults()
	ready := "false"
	if err := Validate(cfg); err == nil {
		ready = "true"
	}
	return map[string]string{
		LabelReady:       ready,
		LabelClusterRoot: cfg.ClusterRoot,
		LabelRootFS:      cfg.RootFS,
	}
}

func Validate(c Config) error {
	cfg := c.WithDefaults()
	if err := validateDir("cluster root", cfg.ClusterRoot); err != nil {
		return err
	}
	if cfg.RequireMount {
		mounted, err := IsMountPoint(cfg.ClusterRoot)
		if err != nil {
			return fmt.Errorf("inspect cluster root mount: %w", err)
		}
		if !mounted {
			return fmt.Errorf("cluster root %s is not a mount point", cfg.ClusterRoot)
		}
	}
	if err := validateDir("SSI rootfs", cfg.RootFS); err != nil {
		return err
	}
	if cfg.Required {
		if err := validateExecutable("SSI executor", cfg.Executor); err != nil {
			return err
		}
	}
	hasBin := pathExists(filepath.Join(cfg.RootFS, "bin")) || pathExists(filepath.Join(cfg.RootFS, "usr/bin"))
	hasLib := pathExists(filepath.Join(cfg.RootFS, "lib")) || pathExists(filepath.Join(cfg.RootFS, "lib64")) || pathExists(filepath.Join(cfg.RootFS, "usr/lib"))
	if !hasBin || !hasLib {
		return fmt.Errorf("SSI rootfs %s does not look bootstrapped: missing executable or library directories", cfg.RootFS)
	}
	return nil
}

func validateDir(name, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s %s is unavailable: %w", name, path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %s is not a directory", name, path)
	}
	return nil
}

func validateExecutable(name, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s %s is unavailable: %w", name, path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%s %s is a directory", name, path)
	}
	if info.Mode()&0111 == 0 {
		return fmt.Errorf("%s %s is not executable", name, path)
	}
	return nil
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func IsMountPoint(path string) (bool, error) {
	target, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	target = filepath.Clean(target)

	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		if unescapeMountInfo(fields[4]) == target {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func ResolveClusterCWD(clusterRoot, rootFS, cwd string) (hostDir, logicalDir string, err error) {
	if clusterRoot == "" {
		clusterRoot = DefaultClusterRoot
	}
	if rootFS == "" {
		rootFS = clusterRoot
	}
	clusterRoot = filepath.Clean(clusterRoot)
	rootFS = filepath.Clean(rootFS)
	cwd = strings.TrimSpace(cwd)
	if cwd == "" || cwd == "." {
		cwd = "/"
	}
	if !filepath.IsAbs(cwd) {
		cwd = "/" + cwd
	}
	clean := filepath.Clean(cwd)
	if clean == "/cluster" || strings.HasPrefix(clean, "/cluster/") {
		rel := strings.TrimPrefix(clean, "/cluster")
		if rel == "" {
			rel = "."
		} else {
			rel = strings.TrimPrefix(rel, "/")
		}
		logical := "/" + strings.TrimPrefix(rel, ".")
		if logical == "/" || logical == "/." {
			logical = "/"
		}
		return filepath.Join(clusterRoot, rel), filepath.Clean(logical), nil
	}
	rel := strings.TrimPrefix(clean, "/")
	if rel == "" {
		return rootFS, "/", nil
	}
	return filepath.Join(rootFS, rel), clean, nil
}

func MergeLabels(dst map[string]string, src map[string]string) map[string]string {
	if dst == nil {
		dst = make(map[string]string, len(src))
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func IsReady(labels map[string]string) bool {
	return labels == nil || labels[LabelReady] != "false"
}

func unescapeMountInfo(s string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(s)
}
