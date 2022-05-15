package main

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestDequeue(t *testing.T) {
	// Dequeue should return 'done' messages, in the order theyu were enqueued

	w := WaitQueue{ctx: context.Background()}
	m1 := kafka.Message{Offset: 0}
	d1 := make(chan bool, 1)
	m2 := kafka.Message{Offset: 1}
	d2 := make(chan bool, 1)
	m3 := kafka.Message{Offset: 2}
	d3 := make(chan bool, 1)

	w.Enqueue(m1, d1)
	w.Enqueue(m2, d2)
	w.Enqueue(m3, d3)

	d := w.Dequeue()

	go func() {
		d2 <- true
		d3 <- false
		d1 <- true
	}()
	am1 := <-d
	if am1.Offset != m1.Offset {
		t.Errorf("First message expected to be %d; was %d", m1.Offset, am1.Offset)
	}
	am2 := <-d
	if am2.Offset != m2.Offset {
		t.Errorf("Second message expected to be %d; was %d", m2.Offset, am2.Offset)
	}
	am3 := <-d
	if am3.Offset != m3.Offset {
		t.Errorf("Third message expected to be %d; was %d", m3.Offset, am3.Offset)
	}

}
