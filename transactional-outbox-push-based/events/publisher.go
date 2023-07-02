package events

import "log"

type Publisher struct {
}

type PublisherOptions struct {
	//opts there like rabbitMQ
}

func (p *Publisher) Publish(event Event) {
	//Send event to message bus or queue
	log.Printf("Event has been published [%v]", event)
}
