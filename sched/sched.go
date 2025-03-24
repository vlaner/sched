package sched

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type Scheduler struct {
	workers int

	handlers map[string]HandlerFunc
	mu       sync.RWMutex

	done chan struct{}
	wg   sync.WaitGroup

	tasks  map[string]Task
	taskCh chan string

	errorHandler func(task Task, err error)
}

func defaultErrorHandler(task Task, err error) {
	log.Println("encountered task error:", task, err)
}

type HandlerFunc func(context.Context, any) error

func NewScheduler(workers int) *Scheduler {
	return &Scheduler{
		handlers:     make(map[string]HandlerFunc),
		workers:      workers,
		wg:           sync.WaitGroup{},
		done:         make(chan struct{}),
		tasks:        make(map[string]Task),
		taskCh:       make(chan string, 1),
		errorHandler: defaultErrorHandler,
	}
}

func (s *Scheduler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.scheduleLoop()
	}()

	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.worker(workerID)
		}(i)
	}

}

func (s *Scheduler) Stop(ctx context.Context) error {
	close(s.done)

	wait := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(wait)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wait:
		return nil
	}
}

func (s *Scheduler) Register(taskName string, handler HandlerFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.handlers[taskName]
	if exists {
		return errors.New("handler already registered")
	}

	s.handlers[taskName] = handler
	return nil
}

func (s *Scheduler) AddTask(task Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.handlers[task.Name]
	if !exists {
		return errors.New("handler does not exist")
	}

	s.tasks[task.ID] = task

	return nil
}

func (s *Scheduler) scheduleLoop() {
	// TODO: use broadcast
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			var taskIDsToRun []string
			s.mu.RLock()
			now := time.Now().UTC()
			for _, task := range s.tasks {
				if now.After(task.NextRunAt) {
					taskIDsToRun = append(taskIDsToRun, task.ID)
				}
			}
			s.mu.RUnlock()

			for _, taskID := range taskIDsToRun {
				s.taskCh <- taskID
			}
		}
	}
}

func (s *Scheduler) worker(id int) {
	for {
		select {
		case <-s.done:
			return
		case taskID := <-s.taskCh:
			s.handleTask(id, taskID)
		}
	}
}

func (s *Scheduler) handleTask(workerID int, taskID string) {
	s.mu.RLock()
	task, exists := s.tasks[taskID]
	if !exists {
		return
	}

	handler, exists := s.handlers[task.Name]
	if !exists {
		return
	}
	s.mu.RUnlock()

	err := handler(context.Background(), task.Payload)
	if err != nil {
		if task.MaxRetries == 0 {
			s.errorHandler(task, err)
		} else if task.MaxRetries > 0 && task.Retries < task.MaxRetries {
			task.Retries++
			s.mu.Lock()
			s.tasks[taskID] = task
			s.mu.Unlock()
			return
		} else {
			s.errorHandler(task, fmt.Errorf("max retries reached: %w", err))
		}
	}

	s.mu.Lock()
	delete(s.tasks, taskID)
	s.mu.Unlock()

	if task.Recurring {
		newTask := task.copy()
		s.mu.Lock()
		s.tasks[newTask.ID] = newTask
		s.mu.Unlock()
	}
}

func generateID() string {
	return rand.Text()
}
