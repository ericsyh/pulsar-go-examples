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
	var msgNumber int

	flag.StringVar(&pulsarUSL, "u", "", "")
	flag.StringVar(&topicName, "t", "", "")
	flag.IntVar(&msgNumber, "n", 1, "")
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

	for i := 0; i < msgNumber; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Properties: map[string]string{
				"foo": "bar",
			},
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgId)
		}
	}
}
