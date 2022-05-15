package main

import (
	"log"
	"sync"
	"time"

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
	logger      *log.Logger
}

// NewDispather returns a new Dispatcher ready to be used
func NewDispather(dest Sender, logger *log.Logger) *Dispatcher {
	return &Dispatcher{
		destination: dest,
		chanByUser:  make(map[string]chan trackedPayload),
		logger:      logger,
	}
}

// Dispatch registers a user event to be dispatched.
// Once the user event gets dispatched, a 'true' value is sent to the 'done' channel
// and the channel is closed immediatetly
func (d *Dispatcher) Dispatch(ue model.UserEvent, done chan<- bool) {
	d.mu.Lock()
	if _, ok := d.chanByUser[ue.UserID]; !ok {
		d.logger.Printf("Creating channel and go routine for user %v\n", ue.UserID)
		ch := make(chan trackedPayload, 100)
		d.chanByUser[ue.UserID] = ch
		go d.send(ue.UserID, ch)
	}
	d.chanByUser[ue.UserID] <- trackedPayload{payload: ue.Payload, done: done}
	d.mu.Unlock()
}

// send is reading event payloads from a user's channel and sends them to the destination,
// once the event is sent (either successfuly or not),  a 'true' value is sent to the 'done' channel
// and the channel is closed immediatetly
// If no event is received for a user for more than 15 seconds, the 'releaseChannel' method is
// called, that will close the user's channel, which in turn will cause the send loop to finish
func (d *Dispatcher) send(userID string, ch <-chan trackedPayload) {
	reset := false
	for {
		select {
		case tp, ok := <-ch:
			if !ok {
				d.logger.Printf("Exiting go routine of user: %v\n", userID)
				return
			}
			reset = true
			err := d.destination.Send(model.UserEvent{UserID: userID, Payload: tp.payload})
			if err != nil {
				d.logger.Printf("Failed to sent event for user %v: %v\n", userID, err)
			}
			tp.done <- err == nil
			close(tp.done)
		case <-time.After(15 * time.Second):
			if reset {
				reset = false
				continue
			}
			d.logger.Printf("Releasing channel for user: %v\n", userID)
			go d.releaseChannel(userID)
		}

	}
}

// releaseChannel closes the user's channel and removes the user key from the map.
// It is meant as a way of cleaning up resources that are no longer needed.
func (d *Dispatcher) releaseChannel(userID string) {
	d.mu.Lock()
	if ch, ok := d.chanByUser[userID]; ok {
		d.logger.Printf("Closing channel for user: %v\n", userID)
		close(ch)
	}
	d.logger.Printf("Deleting map entry for user: %v\n", userID)
	delete(d.chanByUser, userID)
	d.mu.Unlock()
}
