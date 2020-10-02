package sacura

import (
	"fmt"
	"net/http"

	ceformat "github.com/cloudevents/sdk-go/v2/binding/format"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func NewTargeterGenerator(targetURL string, out chan<- string) vegeta.Targeter {

	return func(target *vegeta.Target) error {

		id := uuid.New().String()

		event := cetest.FullEvent()
		event.SetID(id)

		hdr := http.Header{}
		hdr.Set(cehttp.ContentType, ceformat.JSON.MediaType())

		body, err := ceformat.JSON.Marshal(&event)
		if err != nil {
			return fmt.Errorf("failed to marshal event %v: %w", event, err)
		}

		*target = vegeta.Target{
			Method: "POST",
			URL:    targetURL,
			Body:   body,
			Header: hdr,
		}

		out <- id

		return nil
	}
}
