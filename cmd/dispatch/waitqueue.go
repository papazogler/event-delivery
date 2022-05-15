package main

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type message struct {
	kafkaMsg   kafka.Message
	dispatched <-chan bool
}

// WaitQueue is a FIFO queue that only messages that are processed (done) can be dequeued.
//
// It is used to coordinate when a kafka message will be commited.
type WaitQueue struct {
	messages []message //TODO a linked-list might be more efficient here
	ctx      context.Context
	mu       sync.Mutex
}

// Enqueue adds a kafka message to the tail of the queue, along with a done channel
// on which the process outcome of that messages will be received.
func (q *WaitQueue) Enqueue(msg kafka.Message, done <-chan bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, message{msg, done})
}

// Dequeue returns a channel of processed messages.
//
// The messages will be returned in the order they were enqueued, and
// only when they are processed.
func (q *WaitQueue) Dequeue() <-chan kafka.Message {
	ch := make(chan kafka.Message, 10)
	go func() {
		for {
			select {
			case <-q.ctx.Done():
				close(ch)
				return
			case <-q.head().dispatched:
				q.mu.Lock()
				m := q.messages[0].kafkaMsg
				q.messages = q.messages[1:]
				q.mu.Unlock()
				ch <- m
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	return ch
}

func (q *WaitQueue) head() message {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages) == 0 {
		return message{kafka.Message{Offset: -1}, nil}
	}
	return q.messages[0]
}
