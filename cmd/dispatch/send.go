package main

import (
	"log"
	"math"
	"time"

	"github.com/papazogler/event-delivery/model"
)

// Sender is the interface that wraps the basic Send method.
//
// Send sends the user event.
// It returns nil if it succeeds, or a non-nil error if it fails.
type Sender interface {
	Send(ue model.UserEvent) error
}

type SenderFunc func(ue model.UserEvent) error

func (f SenderFunc) Send(ue model.UserEvent) error {
	return f(ue)
}

// An InsistentSenter wraps a Sender and retries a number of times to send failed events.
// It implements an exponential back-off algorithm to push apart repeated retries, and has
// a limit (RetryNumber) on how many times it will retry to send an event.
// On each retry i, the wait period is the min of (2^i * InitialDelay) and MaximumDelay.
type InsistentSender struct {
	RetryNumber  int
	InitialDelay time.Duration
	MaximumDelay time.Duration
	Sender       Sender
	ErrorLogger  *log.Logger
}

// Send makes a number of attempts to send the event.
// If the event cannot be sent after that number, it returns the error of the last attempt.
func (rs InsistentSender) Send(ue model.UserEvent) error {
	var err error
	for i := 0; i < rs.RetryNumber; i++ {
		err = rs.Sender.Send(ue)
		if err == nil {
			break
		}
		if rs.ErrorLogger != nil {
			rs.ErrorLogger.Printf("Failed to sent user event %v, attempts: %d/%d\n", ue, i+1, rs.RetryNumber)
		}
		rs.backoff(i)
	}
	return err
}

func (rs InsistentSender) backoff(retry int) {
	backoff := time.Duration(int32(math.Pow(2, float64(retry)))) * rs.InitialDelay
	time.Sleep(min(backoff, max(rs.InitialDelay, rs.MaximumDelay)))
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
