# How to use
1. Create scheduler with worker amount
```go
scheduler := sched.NewScheduler(3)
scheduler.Start()
```
2. Register handlers
```go
type AddPayload struct {
	a int
	b int
}

scheduler.Register("add", func(ctx context.Context, payload any) error {
    data := payload.(AddPayload)
    fmt.Println(payload, data.a+data.b)
    return nil
})
```
3. Add a task with payload
```go
scheduler.AddTask(sched.NewTask("add", AddPayload{1, 2}))
```

## Options
1. Add recurring task with interval
```go
scheduler.AddTask(sched.NewTask("add", AddPayload{1, 2}, sched.WithRecurring(1*time.Second)))
```
2. Add retries
```go
scheduler.AddTask(sched.NewTask("add", AddPayload{1, 2}, sched.WithRetry(5)))
```

Or combine options
```go
scheduler.AddTask(
    sched.NewTask("add", AddPayload{1, 2},
        sched.WithRetry(5),
        sched.WithRecurring(1*time.Second)),
)
```