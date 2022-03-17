package alacli

import (
	"context"
	"time"

	"github.com/alsve/ala/logger"

	"github.com/streadway/amqp"
)

const DefaultTimeoutDuration = 8500 * time.Millisecond // default to 8.5 second.

// ChannelRenewer shall create connection to AMQP Message Broker.
type ChannelRenewer interface {
	RenewAMQPChannel() (*amqp.Channel, error)
}

// CorrelatorCreateDeleter shall creates or delete a correlation id that
// manages reply channel for RPC usage.
type CorrelatorCreateDeleter interface {
	NewCorrelationID() (correlationID string)
	Delete(correlationID string) bool
}

// ReplyChannelRetriever shall retrieves reply channel by correlation id.
type ReplyChannelRetriever interface {
	ReplyChan(correlationID string) <-chan amqp.Delivery
}

// AMQPClient can be initialized dirrectly as struct initializer
// as its needed variable dependency has been made public.
// i.e: &AMQPClient{}
type AMQPClient struct {
	AMQPCh *amqp.Channel

	// These following are optionals.
	Log                     logger.Logger
	ChannelRenewer          ChannelRenewer
	CorrelatorCreateDeleter CorrelatorCreateDeleter
	ReplyChannelRetriever   ReplyChannelRetriever

	isRenewCalled bool
}

// log is a JIT(just-in-time) dependency injection for Log.
func (a *AMQPClient) log() logger.Logger {
	if a.Log == nil {
		a.Log = logger.L
	}

	return a.Log
}

// getCR is a JIT(just-in-time) dependency injection for ChannelRenewer.
func (a *AMQPClient) getCR() ChannelRenewer {
	if a.ChannelRenewer == nil {
		a.ChannelRenewer = noopChannelRenewer{}
	}

	return a.ChannelRenewer
}

// getCCD is a JIT(just-in-time) dependency injection for CorrelatorCreateDeleter.
func (a *AMQPClient) getCCD() CorrelatorCreateDeleter {
	if a.CorrelatorCreateDeleter == nil {
		a.CorrelatorCreateDeleter = noopCorrelatorCreateDeleter{}
	}

	return a.CorrelatorCreateDeleter
}

// reconnect renew the connection.
func (a *AMQPClient) renewChannel() error {
	ch, err := a.getCR().RenewAMQPChannel()
	if err != nil {
		a.log().Error("AMQPClient.renewChannel: %s", err.Error())
		return err
	}

	a.AMQPCh = ch
	a.isRenewCalled = true
	return nil
}

// Do creates an async or sync call to the destined service.
func (a *AMQPClient) Do(ctx context.Context, exchange string, key string, msg amqp.Publishing) (amqp.Delivery, error) {
	if a.AMQPCh == nil {
		a.log().Error("AMQPClient.Do: channel is nil, %s", ErrPublishFailed)
		return amqp.Delivery{}, nil
	}

	cid := a.getCCD().NewCorrelationID()
	defer a.getCCD().Delete(cid)

	// if failing to publish, try renew channel and try to publish once more.
	msg.CorrelationId = cid
	err := a.AMQPCh.Publish(exchange, key, false, false, msg)
	if err != nil {
		a.log().Error("AMQPClient.Do: %s", err.Error())

		if errRCh := a.renewChannel(); errRCh != nil {
			a.log().Error("AMQPClient.Do: %s", errRCh.Error())
			return amqp.Delivery{}, ErrPublishFailed
		}

		err := a.AMQPCh.Publish(exchange, key, false, false, msg)
		if err != nil {
			a.log().Error("AMQPClient.Do: %s", err.Error())
			return amqp.Delivery{}, ErrPublishFailed
		}
	}

	if a.ReplyChannelRetriever == nil {
		return amqp.Delivery{}, nil
	}

	replyCh := a.ReplyChannelRetriever.ReplyChan(cid)

	select {
	case d := <-replyCh:
		return d, nil
	case <-ctx.Done():
	}

	return amqp.Delivery{}, ErrPublishFailed
}

// Close closes recreated channel.
func (a *AMQPClient) Close() {
	if a.isRenewCalled {
		a.AMQPCh.Close()
	}
}

type noopChannelRenewer struct{}

func (noopChannelRenewer) RenewAMQPChannel() (*amqp.Channel, error) { return nil, ErrCreateChanFailed }

type noopCorrelatorCreateDeleter struct{}

func (noopCorrelatorCreateDeleter) NewCorrelationID() (correlationID string) { return }
func (noopCorrelatorCreateDeleter) Delete(correlationID string) bool         { return true }
