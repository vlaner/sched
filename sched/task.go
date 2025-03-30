package sched

import "time"

var maxBackoff = 5 * time.Second

type TaskStatus string

var (
	PENDING     TaskStatus = "pending"
	RUNNING     TaskStatus = "running"
	RESCHEDULED TaskStatus = "rescheduled"
	FINISHED    TaskStatus = "finished"
	CANCELLED   TaskStatus = "cancelled"
	TIMEOUT     TaskStatus = "timeout"
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

	Timeout time.Duration

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

func WithNextRunAt(at time.Time) TaskOpt {
	return func(task *Task) {
		task.NextRunAt = at
	}
}

func WithID(id string) TaskOpt {
	return func(task *Task) {
		task.ID = id
	}
}

func WithTimeout(timeout time.Duration) TaskOpt {
	return func(task *Task) {
		if timeout > 0 {
			task.Timeout = timeout
		}
	}
}

func NewTask(name string, payload any, opts ...TaskOpt) Task {
	t := Task{
		Name:      name,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
		Status:    PENDING,
	}

	for _, o := range opts {
		o(&t)
	}

	if t.ID == "" {
		t.ID = generateID()
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

func (t *Task) Cancel(at time.Time) {
	t.EndedAt = &at
	t.Status = CANCELLED
}

func (t *Task) Overdue(at time.Time) {
	t.EndedAt = &at
	t.Status = TIMEOUT
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
