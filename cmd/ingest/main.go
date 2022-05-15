package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/papazogler/event-delivery/model"
	kafka "github.com/segmentio/kafka-go"
)

const port = 8090

var kafkaURL, kafkaTopic string
var kafkaWriter *kafka.Writer

func init() {
	flag.StringVar(&kafkaURL, "url", "localhost:9092", "URL to the kafka broker")
	flag.StringVar(&kafkaTopic, "topic", "user-events", "the kafka topic to write to")
}

func main() {
	flag.Parse()
	fmt.Println("Connecting to kafka...")
	kafkaWriter = newKafkaWriter()
	fmt.Printf("Connected to kafka: [%s] topic: [%s]\n", kafkaURL, kafkaTopic)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		_ = <-sigs
		closeKafkaWriter()
		os.Exit(2)
	}()

	fmt.Printf("Listening events on :%v\n", port)
	http.HandleFunc("/events", acceptEvent)
	http.ListenAndServe(fmt.Sprint(":", port), nil)
}

func newKafkaWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaURL),
		Topic:        kafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 10 * time.Millisecond,
		Logger:       kafka.LoggerFunc(logf),
		ErrorLogger:  kafka.LoggerFunc(logf),
	}
}

func logf(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}

func closeKafkaWriter() {
	fmt.Printf("\nClosing kafka writer...\n")
	kafkaWriter.Close()
}

func acceptEvent(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		var userEvents []model.UserEvent
		err := json.NewDecoder(req.Body).Decode(&userEvents)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var msgs []kafka.Message
		for _, ue := range userEvents {
			msgs = append(msgs, kafka.Message{
				Key:   []byte(ue.UserID),
				Value: []byte(ue.Payload),
			})
		}
		err = kafkaWriter.WriteMessages(req.Context(), msgs...)
		if err != nil {
			fmt.Print(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
