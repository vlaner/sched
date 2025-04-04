package sched

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrTaskNotFound = errors.New("task not found")

type TaskStore interface {
	Save(ctx context.Context, task *Task) error
	GetByID(ctx context.Context, ID string) (*Task, error)
	Delete(ctx context.Context, ID string) error
	Update(ctx context.Context, taskID string, updateFn func(*Task) error) error

	EarliestRun(ctx context.Context) (time.Time, bool)
	PendingIDs(ctx context.Context, now time.Time) ([]string, error)
	Claim(ctx context.Context, now time.Time, ids ...string) ([]*Task, error)
}

type MemoryStore struct {
	tasks map[string]*Task
	mu    sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tasks: make(map[string]*Task),
		mu:    sync.RWMutex{},
	}
}

func (m *MemoryStore) Save(ctx context.Context, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tasks[task.ID] = task

	return nil
}

func (m *MemoryStore) GetByID(ctx context.Context, ID string) (*Task, error) {
	m.mu.RLock()
	task, exists := m.tasks[ID]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrTaskNotFound
	}

	return task, nil
}

func (m *MemoryStore) Delete(ctx context.Context, ID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tasks, ID)

	return nil
}

func (m *MemoryStore) EarliestRun(ctx context.Context) (time.Time, bool) {
	var hasTasks bool
	var earliestNextRun time.Time

	m.mu.RLock()
	for _, task := range m.tasks {
		if task.Status == PENDING || task.Status == RESCHEDULED {
			hasTasks = true
			if earliestNextRun.IsZero() {
				earliestNextRun = task.NextRunAt
			}
			if task.NextRunAt.Before(earliestNextRun) {
				earliestNextRun = task.NextRunAt
			}
		}
	}
	m.mu.RUnlock()

	return earliestNextRun, hasTasks
}

func (m *MemoryStore) PendingIDs(ctx context.Context, now time.Time) ([]string, error) {
	var pendingIds []string

	m.mu.Lock()
	for _, task := range m.tasks {
		if now.After(task.NextRunAt) && (task.Status == PENDING || task.Status == RESCHEDULED) {
			pendingIds = append(pendingIds, task.ID)
		}
	}
	m.mu.Unlock()

	return pendingIds, nil
}

func (m *MemoryStore) Claim(ctx context.Context, now time.Time, ids ...string) ([]*Task, error) {
	var claimed []*Task

	m.mu.Lock()
	for _, task := range m.tasks {
		if now.After(task.NextRunAt) && (task.Status == PENDING || task.Status == RESCHEDULED) {
			task.Status = RUNNING
			task.StartedAt = &now
			claimed = append(claimed, task)
		}
	}
	m.mu.Unlock()

	return claimed, nil
}

func (m *MemoryStore) Update(ctx context.Context, taskID string, updateFn func(*Task) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, exists := m.tasks[taskID]
	if !exists {
		return ErrTaskNotFound
	}

	err := updateFn(task)
	if err != nil {
		return err
	}
	return nil
}
