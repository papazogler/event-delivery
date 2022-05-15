package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/papazogler/event-delivery/model"
)

func TestInsistentSender_Send(t *testing.T) {

	const expectedAttempts = 3
	actualAttempts := 0
	sender := InsistentSender{
		RetryNumber:  expectedAttempts,
		InitialDelay: 1 * time.Millisecond,
		MaximumDelay: 2 * time.Second,
		Sender:       SenderFunc(func(ue model.UserEvent) error { actualAttempts++; return fmt.Errorf("error") }),
	}

	err := sender.Send(model.UserEvent{UserID: "user 1", Payload: "payload"})

	if expectedAttempts != actualAttempts {
		t.Errorf("Expected to be attempted %d times; attempted %d", expectedAttempts, actualAttempts)
	}

	if err == nil {
		t.Error("Expected error")
	}
}
