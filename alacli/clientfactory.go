package alacli

import "github.com/Alsve/ala/logger"

// NewClientFactory creates a new instance of ClientFactory.
func NewClientFactory(l logger.Logger, cr ChannelRenewer, ccd CorrelatorCreateDeleter, rcr ReplyChannelRetriever) *ClientFactory {
	return &ClientFactory{log: l, cr: cr, ccd: ccd, rcr: rcr}
}

// ClientFactory is used for using centralized Correlator for centralized reply channel,
// better optimized for RPC AMQP Message Broker.
type ClientFactory struct {
	log logger.Logger
	cr  ChannelRenewer
	ccd CorrelatorCreateDeleter
	rcr ReplyChannelRetriever
}

// AMQPClient creates a new instance of AMQPClient.
// May return error when creating channel.
func (c *ClientFactory) AMQPClient() (*AMQPClient, error) {
	ch, err := c.cr.RenewAMQPChannel()
	if err != nil {
		c.log.Error("pkg.ClientFactory.AMQPClient: %s", err.Error())
		return nil, err
	}

	client := &AMQPClient{
		AMQPCh: ch,

		Log:                     c.log,
		ChannelRenewer:          c.cr,
		CorrelatorCreateDeleter: c.ccd,
		ReplyChannelRetriever:   c.rcr,
	}

	return client, nil
}
