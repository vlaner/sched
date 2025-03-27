package sched

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

type Scheduler struct {
	workers int

	handlers map[string]HandlerFunc
	mu       sync.RWMutex

	done chan struct{}
	wg   sync.WaitGroup

	tasks  map[string]*Task
	taskCh chan *Task

	errorHandler func(task *Task, err error)

	scheduleSignal chan struct{}
}

func defaultErrorHandler(task *Task, err error) {
	log.Println("encountered task error:", task, err)
}

type HandlerFunc func(context.Context, any) error

func NewScheduler(workers int) *Scheduler {
	return &Scheduler{
		handlers:       make(map[string]HandlerFunc),
		workers:        workers,
		wg:             sync.WaitGroup{},
		done:           make(chan struct{}),
		tasks:          make(map[string]*Task),
		taskCh:         make(chan *Task, 1),
		errorHandler:   defaultErrorHandler,
		scheduleSignal: make(chan struct{}, 1),
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

	if task.NextRunAt.IsZero() {
		task.NextRunAt = time.Now().UTC()
	}

	s.tasks[task.ID] = &task

	s.signalSchedule()

	return nil
}

func (s *Scheduler) signalSchedule() {
	select {
	case s.scheduleSignal <- struct{}{}:
	default:
	}
}

func (s *Scheduler) scheduleLoop() {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		var hasTasks bool
		var earliestNextRun time.Time

		s.mu.RLock()
		for _, task := range s.tasks {
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
		s.mu.RUnlock()

		// block run loop until has tasks, without consuming CPU power
		var waitDuration time.Duration = math.MaxInt64
		now := time.Now().UTC()
		if hasTasks {
			waitDuration = earliestNextRun.Sub(now)
			if waitDuration < 0 {
				waitDuration = 0
			}
		}

		timer.Reset(waitDuration)

		select {
		case <-s.done:
			return
		// block loop until signal comes
		case <-s.scheduleSignal:
			continue
		case now := <-timer.C:
			now = now.UTC()
			var tasksToRun []*Task

			s.mu.Lock()
			for _, task := range s.tasks {
				if now.After(task.NextRunAt) && (task.Status == PENDING || task.Status == RESCHEDULED) {
					task.Status = RUNNING
					task.StartedAt = &now
					tasksToRun = append(tasksToRun, task)
				}
			}
			s.mu.Unlock()

			for _, task := range tasksToRun {
				select {
				case s.taskCh <- task:
				default:
					log.Println("could not send task to process", task)
					s.mu.Lock()
					task.Status = PENDING
					task.StartedAt = nil
					s.mu.Unlock()
				}
			}

			s.signalSchedule()
		}
	}
}

func (s *Scheduler) worker(id int) {
	for {
		select {
		case <-s.done:
			return
		case task := <-s.taskCh:
			s.handleTask(id, task)
			s.signalSchedule()
		}
	}
}

// TODO: improve synchronization
func (s *Scheduler) handleTask(workerID int, task *Task) {
	s.mu.RLock()
	task, exists := s.tasks[task.ID]
	s.mu.RUnlock()

	if !exists {
		return
	}

	s.mu.RLock()
	handler, exists := s.handlers[task.Name]
	s.mu.RUnlock()
	if !exists {
		return
	}

	err := handler(context.Background(), task.Payload)
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	task.EndedAt = &now

	if err != nil {
		if task.MaxRetries > 0 && task.Retries < task.MaxRetries {
			task.Retries++
			// TODO: backoff
			task.NextRunAt = now
			task.StartedAt = nil
			task.EndedAt = nil
			task.Status = RESCHEDULED
			s.tasks[task.ID] = task
			return
		} else {
			s.errorHandler(task, fmt.Errorf("max retries reached: %w", err))
			delete(s.tasks, task.ID)
		}
	} else {
		delete(s.tasks, task.ID)

		if task.Recurring {
			newTask := task.copy()
			s.tasks[newTask.ID] = newTask
		}
	}
}

func generateID() string {
	return rand.Text()
}
