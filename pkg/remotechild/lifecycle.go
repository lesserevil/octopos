package remotechild

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type ShadowState string

const (
	StateScheduled  = "scheduled"
	StatePending    = "pending"
	StateAuthorized = "authorized"
	StateLaunching  = "launching"
	StateRecovering = "recovering"
	StateRunning    = "running"
	StateStopped    = "stopped"
	StateStopping   = "stopping"
	StateCompleted  = "completed"
	StateFailed     = "failed"
	StateFallback   = "local_fallback"
	StateOrphaned   = "orphaned"
)

type ShadowRecord struct {
	SessionID       string
	ParentJobID     string
	ParentPID       int
	ShadowPID       int
	RemoteJobID     string
	RemoteNodeID    string
	RemoteGlobalPID uint64
	RemoteLocalPID  int
	Command         []string
	State           ShadowState
	StartedAt       time.Time
	FinishedAt      time.Time
	ExitCode        int
	Signal          int
	PlacementReason string
	FallbackReason  string
	FailureReason   string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type AuditEvent struct {
	Time              time.Time
	Event             string
	Decision          string
	SessionID         string
	ParentJobID       string
	ParentPID         int
	ShadowPID         int
	RemoteJobID       string
	RemoteNodeID      string
	Command           []string
	PlacementReason   string
	FallbackReason    string
	AuthFailureReason string
	PeerPID           int
	PeerUID           int
	PeerGID           int
}

func (e AuditEvent) Clone() AuditEvent {
	e.Command = append([]string(nil), e.Command...)
	return e
}

func (r ShadowRecord) Clone() ShadowRecord {
	r.Command = append([]string(nil), r.Command...)
	return r
}

func (r ShadowRecord) Active() bool {
	return !TerminalState(r.State)
}

func TerminalState(state ShadowState) bool {
	switch state {
	case StateCompleted, StateFailed, StateFallback, StateOrphaned:
		return true
	default:
		return false
	}
}

type ListFilter struct {
	SessionID    string
	ParentJobID  string
	RemoteJobID  string
	RemoteNodeID string
	ActiveOnly   bool
}

type Store struct {
	mu               sync.RWMutex
	records          map[string]ShadowRecord
	audit            []AuditEvent
	path             string
	auditPath        string
	maxAudit         int
	lastPersistErr   error
	lastAuditErr     error
	recoveringOnLoad int
}

func NewStore() *Store {
	return &Store{
		records:  make(map[string]ShadowRecord),
		maxAudit: 1024,
	}
}

func NewPersistentStore(path string) (*Store, error) {
	store := NewStore()
	store.path = path
	if path == "" {
		return store, nil
	}
	store.auditPath = path + ".audit.jsonl"
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	var records []ShadowRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	now := time.Now()
	for _, record := range records {
		if record.RemoteJobID == "" {
			continue
		}
		if record.Active() {
			record.State = StateRecovering
			record.FailureReason = "octoposd restarted; waiting for remote child recovery"
			record.UpdatedAt = now
			store.recoveringOnLoad++
		}
		store.records[record.RemoteJobID] = record.Clone()
	}
	if store.recoveringOnLoad > 0 {
		store.persistLocked()
	}
	return store, nil
}

func (s *Store) OrphanedOnLoad() int {
	return 0
}

func (s *Store) RecoveringOnLoad() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recoveringOnLoad
}

func (s *Store) LastPersistError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastPersistErr
}

func (s *Store) LastAuditError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastAuditErr
}

func (s *Store) RecordAudit(event AuditEvent) {
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	event = event.Clone()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxAudit <= 0 {
		s.maxAudit = 1024
	}
	s.audit = append(s.audit, event)
	if len(s.audit) > s.maxAudit {
		copy(s.audit, s.audit[len(s.audit)-s.maxAudit:])
		s.audit = s.audit[:s.maxAudit]
	}
	s.persistAuditLocked(event)
}

func (s *Store) AuditEvents() []AuditEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]AuditEvent, 0, len(s.audit))
	for _, event := range s.audit {
		out = append(out, event.Clone())
	}
	return out
}

func (s *Store) Upsert(record ShadowRecord) ShadowRecord {
	if record.RemoteJobID == "" {
		return record
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, exists := s.records[record.RemoteJobID]
	if exists {
		if record.SessionID == "" {
			record.SessionID = existing.SessionID
		}
		if record.ParentJobID == "" {
			record.ParentJobID = existing.ParentJobID
		}
		if record.ParentPID == 0 {
			record.ParentPID = existing.ParentPID
		}
		if record.ShadowPID == 0 {
			record.ShadowPID = existing.ShadowPID
		}
		if record.RemoteNodeID == "" {
			record.RemoteNodeID = existing.RemoteNodeID
		}
		if record.RemoteGlobalPID == 0 {
			record.RemoteGlobalPID = existing.RemoteGlobalPID
		}
		if record.RemoteLocalPID == 0 {
			record.RemoteLocalPID = existing.RemoteLocalPID
		}
		if len(record.Command) == 0 {
			record.Command = existing.Command
		}
		if record.State == "" {
			record.State = existing.State
		}
		if record.StartedAt.IsZero() {
			record.StartedAt = existing.StartedAt
		}
		if record.FinishedAt.IsZero() {
			record.FinishedAt = existing.FinishedAt
		}
		if record.PlacementReason == "" {
			record.PlacementReason = existing.PlacementReason
		}
		if record.FallbackReason == "" {
			record.FallbackReason = existing.FallbackReason
		}
		if record.FailureReason == "" {
			record.FailureReason = existing.FailureReason
		}
		record.CreatedAt = existing.CreatedAt
	} else if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	s.records[record.RemoteJobID] = record.Clone()
	s.persistLocked()
	return record.Clone()
}

func (s *Store) MarkRunning(remoteJobID string, globalPID uint64, at time.Time) {
	s.MarkRunningWithLocalPID(remoteJobID, globalPID, 0, at)
}

func (s *Store) MarkRunningWithLocalPID(remoteJobID string, globalPID uint64, localPID int, at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[remoteJobID]
	if !ok {
		return
	}
	record.RemoteGlobalPID = globalPID
	if localPID > 0 {
		record.RemoteLocalPID = localPID
	}
	record.State = StateRunning
	record.FailureReason = ""
	if record.StartedAt.IsZero() {
		record.StartedAt = at
	}
	record.UpdatedAt = at
	s.records[remoteJobID] = record
	s.persistLocked()
}

func (s *Store) MarkFinished(remoteJobID string, state ShadowState, exitCode int, signal int, reason string, at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[remoteJobID]
	if !ok {
		return
	}
	record.State = state
	record.ExitCode = exitCode
	record.Signal = signal
	record.FailureReason = reason
	record.FinishedAt = at
	record.UpdatedAt = at
	s.records[remoteJobID] = record
	s.persistLocked()
}

func (s *Store) MarkState(remoteJobID string, state ShadowState, reason string, at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[remoteJobID]
	if !ok {
		return
	}
	record.State = state
	record.FailureReason = reason
	record.UpdatedAt = at
	s.records[remoteJobID] = record
	s.persistLocked()
}

func (s *Store) Get(remoteJobID string) (ShadowRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[remoteJobID]
	return record.Clone(), ok
}

func (s *Store) List(filter ListFilter) []ShadowRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ShadowRecord, 0, len(s.records))
	for _, record := range s.records {
		if filter.SessionID != "" && record.SessionID != filter.SessionID {
			continue
		}
		if filter.ParentJobID != "" && record.ParentJobID != filter.ParentJobID {
			continue
		}
		if filter.RemoteJobID != "" && record.RemoteJobID != filter.RemoteJobID {
			continue
		}
		if filter.RemoteNodeID != "" && record.RemoteNodeID != filter.RemoteNodeID {
			continue
		}
		if filter.ActiveOnly && !record.Active() {
			continue
		}
		out = append(out, record.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (s *Store) CountActive(filter ListFilter) int {
	filter.ActiveOnly = true
	return len(s.List(filter))
}

func (s *Store) MarkParentTerminal(parentJobID string, state ShadowState, reason string, at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, record := range s.records {
		if record.ParentJobID != parentJobID || !record.Active() {
			continue
		}
		record.State = state
		record.FailureReason = reason
		record.FinishedAt = at
		record.UpdatedAt = at
		s.records[id] = record
	}
	s.persistLocked()
}

func (s *Store) PruneTerminal(cutoff time.Time) int {
	if cutoff.IsZero() {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pruned := 0
	for id, record := range s.records {
		if record.Active() {
			continue
		}
		timestamp := record.FinishedAt
		if timestamp.IsZero() {
			timestamp = record.UpdatedAt
		}
		if timestamp.IsZero() {
			timestamp = record.CreatedAt
		}
		if !timestamp.IsZero() && timestamp.Before(cutoff) {
			delete(s.records, id)
			pruned++
		}
	}
	if pruned > 0 {
		s.persistLocked()
	}
	return pruned
}

func (s *Store) persistLocked() {
	if s.path == "" {
		return
	}
	records := make([]ShadowRecord, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, record.Clone())
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		s.lastPersistErr = err
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".*.tmp")
	if err != nil {
		s.lastPersistErr = err
		return
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(records); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		s.lastPersistErr = err
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		s.lastPersistErr = err
		return
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		_ = os.Remove(tmpName)
		s.lastPersistErr = err
		return
	}
	s.lastPersistErr = nil
}

func (s *Store) persistAuditLocked(event AuditEvent) {
	if s.auditPath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(s.auditPath), 0755); err != nil {
		s.lastAuditErr = err
		return
	}
	file, err := os.OpenFile(s.auditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		s.lastAuditErr = err
		return
	}
	enc := json.NewEncoder(file)
	if err := enc.Encode(event.Clone()); err != nil {
		_ = file.Close()
		s.lastAuditErr = err
		return
	}
	if err := file.Close(); err != nil {
		s.lastAuditErr = err
		return
	}
	s.lastAuditErr = nil
}
