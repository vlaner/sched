package sched

import "time"

var maxBackoff = 5 * time.Second

type TaskStatus string

var (
	PENDING     TaskStatus = "pending"
	RUNNING     TaskStatus = "running"
	RESCHEDULED TaskStatus = "rescheduled"
	FINISHED    TaskStatus = "finished"
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

func (t *Task) Reschedule() {
	t.Retries++
	t.NextRunAt = backoff(time.Now().UTC(), t.Retries)
	t.StartedAt = nil
	t.EndedAt = nil
	t.Status = RESCHEDULED
}

func (t *Task) End(at time.Time) {
	t.EndedAt = &at
	t.Status = FINISHED
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

func backoff(initial time.Time, attempt int) time.Time {
	delay := 300 * time.Millisecond * (1 << uint(attempt))
	if delay > maxBackoff {
		delay = maxBackoff
	}

	return initial.Add(delay)
}
