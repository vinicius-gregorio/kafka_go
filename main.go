package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func main() {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "topic_poc",
	}

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte("message   " + time.Now().GoString()),
		Headers: []protocol.Header{
			{
				Key:   "banana",
				Value: []byte("any_message"),
			},
		},
	})
	if err != nil {
		log.Fatal("Couldn't send message", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "consumer",
		Topic:    "topic_poc",
		MinBytes: 0,
		MaxBytes: 10e6,
	})

	counter := 1

	for counter == 1 {

		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Couldn't read message", err)
			reader.Close()
		}
		for _, v := range message.Headers {
			if v.Key == "banana" {
				fmt.Println("correct header")
			}
		}
		fmt.Println("Received a message: ", string(message.Value))
		counter = 99
	}
	reader.Close()
}
