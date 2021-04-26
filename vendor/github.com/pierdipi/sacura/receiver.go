package sacura

import (
	"context"
	"fmt"
	"log"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	ceclient "github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
)

func StartReceiver(ctx context.Context, config ReceiverConfig, received chan<- string) error {
	defer close(received)

	protocol, err := cehttp.New(cehttp.WithPort(config.Port))
	if err != nil {
		return fmt.Errorf("failed to create protocol: %w", err)
	}

	client, err := ceclient.New(protocol)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	innerCtx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()

		<-ctx.Done()
		if err := ctx.Err(); err != nil {
			log.Println(err)
		}
		<-time.After(config.ParsedTimeout)
	}()

	err = client.StartReceiver(innerCtx, func(ctx context.Context, event ce.Event) {
		received <- event.ID()
	})
	if err != nil {
		select {
		case <-innerCtx.Done():
			// There is not way ATM to know whether the receiver has been terminated because of the cancelled context or
			// because there was an error, so if context is done suppress the error.
			log.Println(err)
		default:
			return fmt.Errorf("failed to start receiver: %w", err)
		}
	}

	return nil
}
