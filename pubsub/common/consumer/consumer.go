package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	var pulsarUSL string
	var topicName string
	var subName string

	flag.StringVar(&pulsarUSL, "u", "", "")
	flag.StringVar(&topicName, "t", "", "")
	flag.StringVar(&subName, "s", "", "")
	flag.Parse()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarUSL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	for true {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received message msgId: %#v -- content: '%s'",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}
	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
}
