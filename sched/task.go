package sched

import "time"

type TaskStatus string

var (
	PENDING     TaskStatus = "pending"
	RUNNING     TaskStatus = "running"
	RESCHEDULED TaskStatus = "rescheduled"
)

type Task struct {
	ID   string
	Name string

	Retries    int
	MaxRetries int

	NextRunAt time.Time
	CreatedAt time.Time

	StartedAt *time.Time
	EndedAt   *time.Time

	Interval  time.Duration
	Recurring bool

	Payload any

	Status TaskStatus
}

type TaskOpt func(*Task)

func WithRetry(maxRetries int) TaskOpt {
	return func(task *Task) {
		task.MaxRetries = maxRetries
	}
}

func WithRecurring(interval time.Duration) TaskOpt {
	return func(task *Task) {
		task.Recurring = true
		task.Interval = interval
	}
}

func NewTask(name string, payload any, opts ...TaskOpt) Task {
	t := Task{
		ID:        generateID(),
		Name:      name,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
		Status:    PENDING,
	}

	for _, o := range opts {
		o(&t)
	}

	return t
}

func (t *Task) copy() *Task {
	now := time.Now().UTC()
	task := Task{
		ID:         generateID(),
		Name:       t.Name,
		MaxRetries: t.MaxRetries,
		CreatedAt:  now,
		Interval:   t.Interval,
		Recurring:  t.Recurring,
		Payload:    t.Payload,
		Status:     PENDING,
	}

	if task.Recurring {
		task.NextRunAt = now.Add(task.Interval)
	}

	return &task
}
