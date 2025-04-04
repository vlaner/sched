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

	runningTasks map[string]context.CancelFunc
	runningMu    sync.Mutex

	wg sync.WaitGroup

	tasks  map[string]*Task
	taskCh chan *Task

	errorHandler func(task *Task, err error)

	scheduleSignal chan struct{}

	store TaskStore

	ctx    context.Context
	cancel context.CancelFunc
}

func defaultErrorHandler(task *Task, err error) {
	log.Println("encountered task error:", task, err)
}

type HandlerFunc func(context.Context, any) error

type SchedulerOpt func(*Scheduler)

func WithStore(store TaskStore) SchedulerOpt {
	return func(s *Scheduler) {
		s.store = store
	}
}

func NewScheduler(workers int, opts ...SchedulerOpt) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := Scheduler{
		handlers:       make(map[string]HandlerFunc),
		runningTasks:   make(map[string]context.CancelFunc),
		workers:        workers,
		wg:             sync.WaitGroup{},
		tasks:          make(map[string]*Task),
		taskCh:         make(chan *Task, 1),
		errorHandler:   defaultErrorHandler,
		scheduleSignal: make(chan struct{}, 1),
		ctx:            ctx,
		cancel:         cancel,
	}

	for _, o := range opts {
		o(&s)
	}

	if s.store == nil {
		s.store = NewMemoryStore()
	}

	return &s
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
			s.worker(s.ctx, workerID)
		}(i)
	}

}

func (s *Scheduler) Stop(ctx context.Context) error {
	s.cancel()

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
	s.mu.RLock()
	_, exists := s.handlers[task.Name]
	if !exists {
		return errors.New("handler does not exist")
	}
	s.mu.RUnlock()

	if task.NextRunAt.IsZero() {
		task.NextRunAt = time.Now().UTC()
	}
	err := s.store.Save(s.ctx, &task)
	if err != nil {
		return err
	}

	s.signalSchedule()

	return nil
}

func (s *Scheduler) CancelTask(taskID string) error {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()

	cancel, exists := s.runningTasks[taskID]
	if !exists {
		return errors.New("cancel task not found")
	}

	cancel()

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
		earliestNextRun, hasTasks := s.store.EarliestRun(s.ctx)

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
		case <-s.ctx.Done():
			return
		// block loop until signal comes
		case <-s.scheduleSignal:
			continue
		case now := <-timer.C:
			now = now.UTC()

			pendingIds, err := s.store.PendingIDs(s.ctx, now)
			if err != nil {
				log.Println("get pending tasks: ", err)
				continue
			}

			now = time.Now().UTC()
			tasksToRun, err := s.store.Claim(s.ctx, now, pendingIds...)

			for _, task := range tasksToRun {
				select {
				case s.taskCh <- task:
				default:
					log.Println("could not send task to process", task)
					updErr := s.store.Update(s.ctx, task.ID, func(t *Task) error {
						task.Status = PENDING
						task.StartedAt = nil
						return nil
					})
					if updErr != nil {
						s.errorHandler(task, fmt.Errorf("scheduled not sent task update: %w", err))
					}

				}
			}

			s.signalSchedule()
		}
	}
}

func (s *Scheduler) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.taskCh:
			s.handleTask(ctx, id, task)
			s.signalSchedule()
		}
	}
}

// TODO: improve synchronization
func (s *Scheduler) handleTask(ctx context.Context, workerID int, t *Task) {
	task, err := s.store.GetByID(s.ctx, t.ID)
	if err != nil {
		// task from Store not found, use one passed to function
		s.errorHandler(t, fmt.Errorf("get task: %w", err))
		return
	}

	s.mu.RLock()
	handler, exists := s.handlers[task.Name]
	s.mu.RUnlock()
	if !exists {
		return
	}

	var handleCtx context.Context
	var handleCancel context.CancelFunc
	if task.Timeout > 0 {
		handleCtx, handleCancel = context.WithTimeout(ctx, task.Timeout)
	} else {
		handleCtx, handleCancel = context.WithCancel(ctx)
	}

	s.runningMu.Lock()
	s.runningTasks[task.ID] = handleCancel
	s.runningMu.Unlock()

	err = handler(handleCtx, task.Payload)
	handleCancel()

	s.runningMu.Lock()
	delete(s.runningTasks, task.ID)
	s.runningMu.Unlock()

	now := time.Now().UTC()

	if err != nil {
		taskCancelled := errors.Is(err, context.Canceled)
		taskDeadline := errors.Is(err, context.DeadlineExceeded)
		updErr := s.store.Update(s.ctx, task.ID, func(t *Task) error {
			if taskCancelled {
				task.Cancel(now)
				return nil
			} else if taskDeadline {
				task.Overdue(now)
				return nil
			} else if task.MaxRetries > 0 && task.Retries < task.MaxRetries {
				t.Reschedule()
				return nil
			}

			t.End(now)
			return nil
		})
		if updErr != nil {
			s.errorHandler(task, fmt.Errorf("task update: %w", err))
		}
		if task.Status == RESCHEDULED {
			return
		} else if task.MaxRetries > 0 && task.Retries == task.MaxRetries {
			s.errorHandler(task, fmt.Errorf("max retries reached: %w", err))
		} else {
			s.errorHandler(task, err)
		}
	}

	delErr := s.store.Delete(s.ctx, task.ID)
	if delErr != nil {
		s.errorHandler(task, fmt.Errorf("task delete: %w", err))
	}

	if task.Recurring {
		newTask := task.copy()
		saveErr := s.store.Save(s.ctx, newTask)
		if saveErr != nil {
			s.errorHandler(newTask, fmt.Errorf("recurring task save: %w", err))
		}
	}

}

func generateID() string {
	return rand.Text()
}
