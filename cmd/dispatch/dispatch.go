package main

import (
	"log"
	"sync"

	"github.com/papazogler/event-delivery/model"
)

type trackedPayload struct {
	payload string
	done    chan<- bool
}

// Dispatcher dispatches user events to a destination,
// making sure that events of one user are dispatched in order
// and they do not block the dispatch of other users' events.
//
// In order to fullfill the ordering and no inter-user blocking,
// each user event is sent to a separate, per-user user, channel.
// Each channel is read by a separate, per-user go routine, that
// dispatches the event to each destination.
//
// A Dispatcher can be used simultaneously from multiple goroutines.
type Dispatcher struct {
	destination Sender
	mu          sync.RWMutex
	chanByUser  map[string]chan trackedPayload
	errorLogger *log.Logger
}

// NewDispather returns a new Dispatcher ready to be used
func NewDispather(dest Sender, errorLogger *log.Logger) *Dispatcher {
	return &Dispatcher{
		destination: dest,
		chanByUser:  make(map[string]chan trackedPayload),
		errorLogger: errorLogger,
	}
}

// Dispatch registers a user event to be dispatched.
// Once the user event gets dispatched, a 'true' value is sent to the 'done' channel
// and the channel is closed immediatetly
func (d *Dispatcher) Dispatch(ue model.UserEvent, done chan<- bool) {
	d.mu.Lock()
	if _, ok := d.chanByUser[ue.UserID]; !ok {
		ch := make(chan trackedPayload, 100)
		d.chanByUser[ue.UserID] = ch
		go d.send(ue.UserID, ch)
	}
	d.mu.Unlock()
	d.chanByUser[ue.UserID] <- trackedPayload{payload: ue.Payload, done: done}
}

// send is reading event payloads from a user's channel and sends them to the destination,
// once the event is sent (either successfuly or not),  a 'true' value is sent to the 'done' channel
// and the channel is closed immediatetly
func (d *Dispatcher) send(userId string, ch <-chan trackedPayload) {
	for {
		tp := <-ch
		err := d.destination.Send(model.UserEvent{UserID: userId, Payload: tp.payload})
		if err != nil {
			d.errorLogger.Printf("Failed to sent event for user %v: %v\n", userId, err)
		}
		tp.done <- err == nil
		close(tp.done)
	}
}
