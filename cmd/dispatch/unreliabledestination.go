package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"

	"github.com/papazogler/event-delivery/model"
)

// unreliableDestination represents a Sender that randomly fails to sent an event.
//
// It generates a random int and if that int is divisible by failOneOutOf,
// it doesn't send the event and it returns an error.
type unreliableDestination struct {
	failOneOutOf int
	writer       io.Writer
	logger       *log.Logger
}

func (d unreliableDestination) Send(ue model.UserEvent) error {
	if d.failOneOutOf > 1 && rand.Int()%d.failOneOutOf == 0 {
		return fmt.Errorf("event error %v\n", ue)
	}
	_, err := fmt.Fprintln(d.writer, ue)
	if err != nil {
		return fmt.Errorf("error writing event: %v\n", err)
	}
	d.logger.Printf("Event sent: %v\n", ue)
	return nil
}
