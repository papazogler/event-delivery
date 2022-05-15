# Event Delivery

Event Delivery is system that receives events from multiple users, and delivers them in multiple destinations.

The system fulfills the following requirements:

- __Durability__, once an event is accepted, it will remain into the system until it is processed, even if the system crashes.
- __At-least-once delivery__, the system guarantees that each event will processed (attempted to be delivered) at least once. In the event of system crashes, already processed events may be reprocessed.
- __Retry backoff and limit__, the system will attempt to deliver an event, a number of times before giving up. It uses an exponential backoff algorithm to separate succecutive attempts.
- __Maintaining order__, events of the same user will be delivered in the order received.
- __Delivery Isolation__, delays or failures with event delivery to a single destination, do not affect the ingestion or delivery to other destinations. Delays or failures with event delivery of a single user do not affect other users' event delivery under the condition that the per-user unprocessed events is under a prespecified threshold.

## Architecture

The system depends on 3 building blocks:

- An __ingest__ application, that accepts http requests of user events and stores them to a kafka topic. It is implemented in Go.
- A __dispatch__ application, that consumes messages from a kafka topic and dispatches them to a __single__ destination. To deliver events into multiple destinations, multiple instances of the dispatch application must be started, one for each destination. It is implemented in Go.
- A __kafka topic__, for storing the user events.

## Pre-Requisites

- install Go [https://go.dev/doc/install](https://go.dev/doc/install)
- install Docker [https://docs.docker.com/engine/install](https://docs.docker.com/engine/install)

## Usage

1. Build the project: `go build -o bin/ ./...`
2. Start kafka: `docker compose up -d`. The docker-compose.yml creates a 'user-events' topic with 3 partitions.
3. Start the `ingest` process: `bin/ingest -url localhost:9092 -topic user-events`. The process spins up an http server, accepting `POST` requests on `http://localhost:8090/events`
4. Start a `dispatch` process per destination: `bin/dispatch -url localhost:9092 -topic user-events -dest bin/file-1 -reliability 5`
5. Send some events:
    ```
    curl --location --request POST 'http://localhost:8090/events' \
    --header 'Content-Type: application/json' \
    --data-raw '[
        {"UserID": "user-1","Payload": "event-1-payload"},
        {"UserID": "user-2","Payload": "event-1-payload"},
        {"UserID": "user-1","Payload": "event-2-payload"},
        {"UserID": "user-3","Payload": "event-1-payload"},
        {"UserID": "user-2","Payload": "event-2-payload"}
    ]'
    ```