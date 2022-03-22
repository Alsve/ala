package alaserv

import (
	"context"
	"fmt"
	"log"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/tidwall/spinlock"
)

// Publisher publish message to amqp message broker.
type Publisher interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// Replyer shall connect correlation id to listener.
type Replyer interface {
	Reply(correlationID string, data amqp.Delivery) error
}

// ConnectReconnector shall provide an active connection and reconnect signal when reconnection occured.
type ConnectReconnector interface {
	Connector
	ReconnectSignaler
}

// Connector shall provide an active connection.
type Connector interface {
	Connect() *amqp.Connection
}

// ReconnectSignaler shall provide reconnect signal when reconnection occured
type ReconnectSignaler interface {
	ReconnectSignal() <-chan *amqp.Connection
}

// HandlerFunc is entry point for receiving Delivery and make publish for the next exchange.
type HandlerFunc func(ctx context.Context, d amqp.Delivery, p Publisher) error

// New creates a new instance of Ala AMQP server.
func New(amqpURI string) *AlaServer {
	as := &AlaServer{
		amqpURI:           amqpURI,
		routeCloseSignals: newCanceler(),
	}

	return as
}

// AlaServer is an AMQP Server.
type AlaServer struct {
	amqpURI         string
	c               Connector
	r               Replyer
	conn            *amqp.Connection
	amqpChan        *amqp.Channel // for publishing
	amqpChanConsume *amqp.Channel // for consuming

	closeSignal       context.CancelFunc
	ctx               context.Context
	routeCloseSignals *canceler

	cmrCloser       chan struct{}           // cmrCloser is connectionMonitorRoutine closer signaler.
	reconnectSignal <-chan *amqp.Connection // reconnectSignal is received when reconnection occured from Connector.

	rhs []routeHandlerSetting

	// ready will locked until all startup conduct is done.
	ready spinlock.Locker

	// nConsumePrefetch is consume prefetch count.
	nConsumePrefetch int
}

// SetReplyer sets replyer for connecting queue to listener.
func (a *AlaServer) SetReplyer(r Replyer) {
	a.ready.Lock()
	defer a.ready.Unlock()

	a.r = r
}

// getReplyer retrieves Replyer.
func (a *AlaServer) getReplyer() Replyer {
	if a.r == nil {
		a.ready.Lock()
		if a.r == nil { // double check like once.
			a.r = noopReplyer{}
		}
		a.ready.Unlock()
	}
	return a.r
}

// SetConnector sets connector for receiving latest active connection from connector.
func (a *AlaServer) SetConnector(c Connector) {
	a.ready.Lock()
	defer a.ready.Unlock()

	a.c = c
}

// SetReconnectSignal sets reconnect signal which give signal when reconnection occured.
func (a *AlaServer) SetReconnectSignal(c ReconnectSignaler) {
	a.ready.Lock()
	defer a.ready.Unlock()
	a.reconnectSignal = c.ReconnectSignal()

	a.closeConnectionMonitorRoutine()
	go a.startConnectionMonitorRoutine()
}

// closeConnectionMonitorRoutine closes connection monitor routine.
func (a *AlaServer) closeConnectionMonitorRoutine() {
	select {
	case a.cmrCloser <- struct{}{}:
	default:
	}
}

// startConnectionMonitorRoutine
func (a *AlaServer) startConnectionMonitorRoutine() {
	for {
		select {
		case <-a.cmrCloser:
			return
		case conn := <-a.reconnectSignal:
			a.ready.Lock()
			a.routeCloseSignals.CancelAll()
			a.conn = conn

			amqpChan, err := conn.Channel()
			if err != nil {
				log.Printf("Failed to create channel: %s\n", err.Error())
			}
			a.amqpChan = amqpChan

			amqpChanConsume, err := conn.Channel()
			if err != nil {
				log.Printf("Failed to create channel: %s\n", err.Error())
			}
			a.amqpChanConsume = amqpChanConsume
			a.amqpChanConsume.Qos(a.nConsumePrefetch, 0, false)

			a.startupServer()
			a.ready.Unlock()
		}
	}
}

// Route register a new route for new handler.
func (a *AlaServer) Route(q QueueSetting, ess []ExchangeSetting, handler HandlerFunc) {
	a.rhs = append(a.rhs, routeHandlerSetting{q: &q, ess: ess, handler: handler})
}

// registerRoute declares queue and needed exchange and return consume channel used for handler routine.
func (a *AlaServer) registerRoute(q QueueSetting, ess []ExchangeSetting) (<-chan amqp.Delivery, error) {
	_, err := a.amqpChan.QueueDeclare(q.Name, !q.NotDurable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args)
	if err != nil {
		log.Printf("Failed to declare queue of %s: %s", q.Name, err.Error())
		return nil, err
	}

	for _, es := range ess {
		err = a.amqpChan.ExchangeDeclare(es.Name, es.kind(), !es.NotDurable, es.AutoDelete, es.Internal, es.NoWait, es.Args)
		if err != nil {
			log.Printf("Failed to declare exchange of %s: %s", es.Name, err.Error())
			return nil, err
		}

		routeKey := es.RouteKey
		if routeKey == "" {
			routeKey = q.Name
		}

		err = a.amqpChan.QueueBind(q.Name, routeKey, es.Name, es.NoWaitQueueBind, es.ArgsQueueBind)
		if err != nil {
			log.Printf("Failed to bind exchange to queue of %s: %s", es.Name, err.Error())
			return nil, err
		}
	}

	consumeCh, err := a.amqpChanConsume.Consume(q.Name, q.ConsumerKey, !q.ManualAck, q.ExclusiveConsume, false, q.NoWaitConsume, q.ArgsConsume)
	if err != nil {
		log.Printf("Failed to consume queue: %s", err.Error())
		return nil, err
	}

	return consumeCh, nil
}

// routineRecover recover from panics that is fired from handler.
func (a *AlaServer) routineRecover(d amqp.Delivery, routineUUIDStr string) {
	if r := recover(); r != nil {
		log.Printf("%s", r)
		d.Nack(false, true)
		a.routeCloseSignals.Delete(routineUUIDStr)
	}
}

// startHandlerRoutine start routine for handler to receive delivery.
func (a *AlaServer) startHandlerRoutine(consumeCh <-chan amqp.Delivery, handler HandlerFunc) {
	for delivery := range consumeCh {
		go func(d amqp.Delivery) {
			routeCtx, cancel := context.WithCancel(a.ctx)
			defer cancel()

			a.getReplyer().Reply(d.CorrelationId, d)

			// setup close routine if successfull or in case of panic.
			routineUUID := uuid.NewV4()
			ruuidStr := routineUUID.String()
			a.routeCloseSignals.Set(ruuidStr, cancel)

			defer a.routineRecover(d, ruuidStr)
			err := handler(routeCtx, d, a.amqpChan)
			if err != nil {
				d.Nack(false, true)
			}

			// delete cancelFunc from singals.
			a.routeCloseSignals.Delete(ruuidStr)
		}(delivery)
	}
}

// startupServer starts server and fires up routine for each registered handler.
func (a *AlaServer) startupServer() {
	for _, rhs := range a.rhs {
		consumeCh, err := a.registerRoute(*rhs.q, rhs.ess)
		if err != nil {
			log.Panicf("failed to register route: %s", err.Error())
		}
		go func(rhs routeHandlerSetting) {
			a.startHandlerRoutine(consumeCh, rhs.handler)
		}(rhs)
	}
}

// Start starts a Ala AMQP Server for this service.
func (a *AlaServer) Start(ctx context.Context) {
	a.ready.Lock()
	if a.conn == nil {
		conn, err := amqp.Dial(a.amqpURI)
		if err != nil {
			log.Panicf("Failed to start server: %s", err.Error())
		}
		a.conn = conn

		amqpChan, err := conn.Channel()
		if err != nil {
			log.Panicf("Failed to create channel: %s", err.Error())
		}
		a.amqpChan = amqpChan

		amqpChanConsume, err := conn.Channel()
		if err != nil {
			log.Panicf("Failed to create channel: %s", err.Error())
		}
		a.amqpChanConsume = amqpChanConsume
		a.amqpChanConsume.Qos(a.nConsumePrefetch, 0, false)
	}

	serverCtx, cancel := context.WithCancel(ctx)
	a.ctx = serverCtx
	a.closeSignal = cancel

	a.startupServer()
	a.printLogo()
	a.ready.Unlock()
	<-serverCtx.Done()
}

// Close closes connection and release the server loop.
func (a *AlaServer) Close() error {
	defer a.closeSignal()

	a.routeCloseSignals.CancelAll()
	a.closeConnectionMonitorRoutine()

	if a.c == nil {
		return a.conn.Close()
	}

	return nil
}

// printLogo prints ala server logo for startup.
func (a *AlaServer) printLogo() {
	uri, _ := amqp.ParseURI(a.amqpURI)
	fmt.Printf(`
  __   __     __   ____  ____  ____  _  _  ____ 
 / _\ (  )   / _\ / ___)(  __)(  _ \/ )( \(  __)
/    \/ (_/\/    \\___ \ ) _)  )   /\ \/ / ) _) 
\_/\_/\____/\_/\_/(____/(____)(__\_) \__/ (____)

Starting publish/consume at vhost of: %s
`+"\n", uri.Vhost)
}

// noopReplyer is a no-op independency injection.
type noopReplyer struct{}

// Reply implements Replyer.
func (noopReplyer) Reply(correlationID string, data amqp.Delivery) error { return nil }
