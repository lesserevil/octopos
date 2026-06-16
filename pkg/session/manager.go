package session

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/namespace"
)

type SessionState int

const (
	SessionActive SessionState = iota
	SessionClosing
	SessionClosed
)

type ManagedSession struct {
	cluster.Session
	State    SessionState
	Jobs     map[cluster.JobID]*cluster.JobInfo
	Created  time.Time
	cgroupNS *namespace.Manager
}

type Manager struct {
	mu       sync.RWMutex
	sessions map[cluster.SessionID]*ManagedSession
	nsMgr    *namespace.Manager
}

func NewManager() (*Manager, error) {
	nsMgr, err := namespace.NewManager()
	if err != nil {
		return nil, fmt.Errorf("namespace manager: %w", err)
	}

	return &Manager{
		sessions: make(map[cluster.SessionID]*ManagedSession),
		nsMgr:    nsMgr,
	}, nil
}

func (m *Manager) Create(sess *cluster.Session) (*ManagedSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[sess.ID]; exists {
		return nil, fmt.Errorf("session %s already exists", sess.ID)
	}

	ms := &ManagedSession{
		Session:  *sess,
		State:    SessionActive,
		Jobs:     make(map[cluster.JobID]*cluster.JobInfo),
		Created:  time.Now(),
		cgroupNS: m.nsMgr,
	}

	if err := m.nsMgr.CreateCgroup(string(sess.ID), 0, 0); err != nil {
		return nil, fmt.Errorf("create cgroup: %w", err)
	}

	m.sessions[sess.ID] = ms
	return ms, nil
}

func (m *Manager) Get(id cluster.SessionID) (*ManagedSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

func (m *Manager) Destroy(id cluster.SessionID) error {
	m.mu.Lock()
	sess, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("session %s not found", id)
	}
	sess.State = SessionClosing
	m.mu.Unlock()

	for jobID, job := range sess.Jobs {
		if job.Status == cluster.JobStatusRunning {
			job.Status = cluster.JobStatusStopped
			job.FinishedAt = time.Now()
			delete(sess.Jobs, jobID)
		}
	}

	m.nsMgr.DestroyCgroup(string(id))

	m.mu.Lock()
	sess.State = SessionClosed
	delete(m.sessions, id)
	m.mu.Unlock()

	return nil
}

func (m *Manager) AddJob(sessionID cluster.SessionID, job *cluster.JobInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sess, ok := m.sessions[sessionID]
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	if sess.State != SessionActive {
		return fmt.Errorf("session %s is not active", sessionID)
	}

	sess.Jobs[job.ID] = job
	return nil
}

func (m *Manager) List() []*ManagedSession {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*ManagedSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		result = append(result, s)
	}
	return result
}

func (m *Manager) ListByNode(nodeID cluster.NodeID) []*ManagedSession {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*ManagedSession
	for _, s := range m.sessions {
		if s.NodeID == nodeID {
			result = append(result, s)
		}
	}
	return result
}

func (m *Manager) Cleanup(ctx context.Context, maxAge time.Duration) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			for id, sess := range m.sessions {
				if sess.State == SessionClosed && time.Since(sess.Created) > maxAge {
					delete(m.sessions, id)
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}
