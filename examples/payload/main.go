package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vlaner/sched/sched"
)

type AddPayload struct {
	a int
	b int
}

func main() {
	scheduler := sched.NewScheduler(3)
	scheduler.Start()

	scheduler.Register("add", func(ctx context.Context, payload any) error {
		data := payload.(AddPayload)
		fmt.Println(payload, data.a+data.b)
		return nil
	})

	scheduler.Register("error", func(ctx context.Context, payload any) error {
		return errors.New("always error")
	})

	scheduler.Register("singleerr", func(ctx context.Context, payload any) error {
		return errors.New("always erro single no retry")
	})

	scheduler.Register("shortrun", func(ctx context.Context, payload any) error {
		<-ctx.Done()
		return ctx.Err()
	})

	scheduler.Register("cancel", func(ctx context.Context, payload any) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
			return errors.New("task was not cancelled after 3 seconds")
		}
	})

	scheduler.AddTask(sched.NewTask("add", AddPayload{1, 2}, sched.WithRecurring(1*time.Second)))
	scheduler.AddTask(sched.NewTask("error", nil, sched.WithRetry(5)))
	scheduler.AddTask(sched.NewTask("singleerr", nil))
	scheduler.AddTask(sched.NewTask("shortrun", nil, sched.WithTimeout(100*time.Millisecond)))

	scheduler.AddTask(
		sched.NewTask("add", AddPayload{100, 100},
			sched.WithID("custom-id"),
			sched.WithNextRunAt(time.Now().Add(10*time.Second))),
	)

	go func() {
		taskToCancel := sched.NewTask("cancel", nil)
		scheduler.AddTask(taskToCancel)
		time.Sleep(1 * time.Second)
		err := scheduler.CancelTask(taskToCancel.ID)
		if err != nil {
			log.Println("cancel:", err)
		}
	}()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit

	scheduler.Stop(context.Background())
}
