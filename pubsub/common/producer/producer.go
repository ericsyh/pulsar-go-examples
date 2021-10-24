package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {

	var pulsarUSL string
	var topicName string

	flag.StringVar(&pulsarUSL, "u", "", "")
	flag.StringVar(&topicName, "t", "", "")
	flag.Parse()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarUSL,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	ctx := context.Background()

	for i := 0; i < 10000; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgId)
		}
	}
}
