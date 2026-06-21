package rpc

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
)

type pipeCoordinator struct {
	mu        sync.Mutex
	placement map[string]cluster.NodeID
	local     map[string]*localPipe
}

type localPipe struct {
	read  *os.File
	write *os.File
}

func newPipeCoordinator() *pipeCoordinator {
	return &pipeCoordinator{
		placement: make(map[string]cluster.NodeID),
		local:     make(map[string]*localPipe),
	}
}

func remoteChildPipeIDsFromEnv(env []string) map[int]string {
	out := make(map[int]string)
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(key, remotechild.EnvPipeFDPrefix) || value == "" {
			continue
		}
		rawFD := strings.TrimPrefix(key, remotechild.EnvPipeFDPrefix)
		fd, err := strconv.Atoi(rawFD)
		if err != nil || fd < 0 || fd > 2 {
			continue
		}
		out[fd] = value
	}
	return out
}

func remoteChildPipeKeys(req *ExecuteRequest) map[int]string {
	ids := remoteChildPipeIDsFromEnv(req.Env)
	if len(ids) == 0 {
		return nil
	}
	out := make(map[int]string, len(ids))
	parentJobID := requestEnvValue(req.Env, remotechild.EnvParentJobID)
	for fd, id := range ids {
		out[fd] = remoteChildPipeKey(req.SessionId, parentJobID, id)
	}
	return out
}

func remoteChildPipeKey(sessionID string, parentJobID string, pipeID string) string {
	return sessionID + "\x00" + parentJobID + "\x00" + pipeID
}

func (p *pipeCoordinator) preferredNode(keys map[int]string) cluster.NodeID {
	if p == nil || len(keys) == 0 {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, key := range keys {
		if nodeID := p.placement[key]; nodeID != "" {
			return nodeID
		}
	}
	return ""
}

func (p *pipeCoordinator) recordPlacement(keys map[int]string, nodeID cluster.NodeID) {
	if p == nil || len(keys) == 0 || nodeID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, key := range keys {
		if p.placement[key] == "" {
			p.placement[key] = nodeID
		}
	}
}

func (p *pipeCoordinator) attachLocal(key string, fd int) (*os.File, error) {
	if p == nil || key == "" {
		return nil, fmt.Errorf("missing pipe coordinator")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	pipe := p.local[key]
	if pipe == nil {
		read, write, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		pipe = &localPipe{read: read, write: write}
		p.local[key] = pipe
	}

	var file *os.File
	var err error
	switch fd {
	case 0:
		if pipe.read == nil {
			return nil, fmt.Errorf("pipe read endpoint already attached")
		}
		file, err = dupFile(pipe.read, "octopos-pipe-read")
		if err == nil {
			_ = pipe.read.Close()
			pipe.read = nil
		}
	case 1, 2:
		if pipe.write == nil {
			return nil, fmt.Errorf("pipe write endpoint already attached")
		}
		file, err = dupFile(pipe.write, "octopos-pipe-write")
		if err == nil {
			_ = pipe.write.Close()
			pipe.write = nil
		}
	default:
		return nil, fmt.Errorf("unsupported pipe fd %d", fd)
	}
	if err != nil {
		return nil, err
	}
	if pipe.read == nil && pipe.write == nil {
		delete(p.local, key)
	}
	return file, nil
}

func dupFile(file *os.File, name string) (*os.File, error) {
	fd, err := syscall.Dup(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}
