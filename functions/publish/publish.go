package main

import (
	"context"
	"errors"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func PublishFunc(ctx context.Context, in []byte) error {
	fctx, ok := pf.FromContext(ctx)
	if !ok {
		return errors.New("get Go Functions Context error")
	}

	publishTopic := "output"

	producer := fctx.NewOutputMessage(publishTopic)
	msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte(string(in) + "!"),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("The output message ID is: %+v", msgID)
	return nil
}

func main() {
	pf.Start(PublishFunc)
}
