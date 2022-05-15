package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/papazogler/event-delivery/model"
	"github.com/segmentio/kafka-go"
)

var kafkaURL, kafkaTopic, destination string
var reliability int
var kafkaReader *kafka.Reader
var logger *log.Logger
var messageQueue *WaitQueue
var dispatcher *Dispatcher

func init() {
	flag.StringVar(&kafkaURL, "url", "localhost:9092", "URL to the kafka broker")
	flag.StringVar(&kafkaTopic, "topic", "user-events", "the kafka topic to read from")
	flag.StringVar(&destination, "dest", "", "the name for the destination, used as the kafka consumer group as well")
	flag.IntVar(&reliability, "reliability", 5, "one event out of this number of events will fail to be delivered")
}

func main() {
	flag.Parse()
	logger = log.New(os.Stderr, "", log.Lmicroseconds|log.Ldate)

	destinationFile, err := os.Create(destination)
	if err != nil {
		fmt.Printf("Could not reach destination: %v", err)
		return
	}
	defer destinationFile.Close()
	defer destinationFile.Sync()

	sender := InsistentSender{
		RetryNumber:  10,
		InitialDelay: 100 * time.Millisecond,
		MaximumDelay: 2 * time.Second,
		ErrorLogger:  logger,
		Sender: unreliableDestination{
			failOneOutOf: reliability,
			writer:       destinationFile,
			logger:       logger,
		},
	}

	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaURL},
		GroupID:     destination,
		Topic:       kafkaTopic,
		ErrorLogger: kafka.LoggerFunc(logf),
	})
	defer kafkaReader.Close()

	ctx, cancelCtx := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		_ = <-sigs
		fmt.Printf("\nExiting...\n")
		cancelCtx()
	}()

	messageQueue = &WaitQueue{messages: make([]message, 0, 100), ctx: ctx}
	dispatcher = NewDispather(sender, logger)

	go process(ctx)
	go commit(ctx)
	<-ctx.Done()
}

func logf(msg string, a ...interface{}) {
	logger.Printf(msg, a...)
	logger.Println()
}

func process(ctx context.Context) {
	for {
		m, err := kafkaReader.FetchMessage(ctx)
		if err != nil {
			break
		}
		ue := model.UserEvent{
			UserID:  string(m.Key),
			Payload: string(m.Value),
		}
		done := make(chan bool, 1)

		fmt.Printf("processing event: %v\n", ue)
		messageQueue.Enqueue(m, done)
		dispatcher.Dispatch(ue, done)
	}
}

func commit(ctx context.Context) {
	processed := messageQueue.Dequeue()
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-processed:
			if err := kafkaReader.CommitMessages(ctx, m); err != nil {
				fmt.Printf("failed to commit messages: %v\n", err)
			}
			fmt.Printf("committed partition: %d at offset: %d\n", m.Partition, m.Offset)
		}
	}
}
