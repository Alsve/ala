package alaserv

import "github.com/streadway/amqp"

// QueueSetting is needed to declare queue for setting up route.
type QueueSetting struct {
	Name       string
	NotDurable bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table

	// Queue Consume setting.
	ManualAck        bool
	ConsumerKey      string
	ExclusiveConsume bool
	NoWaitConsume    bool
	ArgsConsume      amqp.Table
}

// ExchangeSetting is needed to declare exchange for binding queue
// and setting up route or topic.
type ExchangeSetting struct {
	Name       string
	Kind       string
	NotDurable bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table

	// RouteKey a key for binding exchange to the queue provided in
	// the setting up route function.
	RouteKey        string
	NoWaitQueueBind bool
	ArgsQueueBind   amqp.Table
}

// kind return type of exchange. if kind is empty return "direct"
// as default kind.
func (e *ExchangeSetting) kind() string {
	if e.Kind == "" {
		return "direct"
	}

	return e.Kind
}

// routeHandlerSetting is saving handler and its setting for
// firing at start function.
type routeHandlerSetting struct {
	q       *QueueSetting
	ess     []ExchangeSetting
	handler HandlerFunc
}
