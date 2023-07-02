package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/ragoob/distributed-systems/transactional-outbox-push-based/events"
)

func main() {
	slotName := fmt.Sprintf("events_slot%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	publicationName := "outbox_pub"
	e := &events.EventSubscription{
		Opts: &events.EventSubscriptionOptions{
			ConnStr:         os.Getenv("PGLOGREPL_DEMO_CONN_STRING"),
			SlotName:        slotName,
			PublicationName: publicationName,
		},
	}
	p := &events.Publisher{}
	eventChan := e.Subscribe(context.Background())
	for event := range eventChan {
		p.Publish(event)
	}
}
