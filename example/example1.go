package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Alsve/ala/alaserv"
	"github.com/Alsve/ala/connman"
	"github.com/Alsve/ala/logger"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

// New creates new instance of AMQP Server.
func New(l logger.Logger, amqpURI string) *Server {
	ch := NewConfigHandler(l)

	return &Server{
		log:           l,
		amqpURI:       amqpURI,
		configHandler: ch.ServeAMQP,
	}
}

// Server is an AMQP server.
type Server struct {
	log     logger.Logger
	amqpURI string
	server  *alaserv.AlaServer

	configHandler alaserv.HandlerFunc
}

// Listen starts a AMQP rest for this service
func (s *Server) Listen(ctx context.Context) {
	cm := connman.NewAMQPConnectionManager(s.log, s.amqpURI)
	s.server = alaserv.New(s.amqpURI)
	s.server.SetConnector(cm)
	s.server.SetReconnectSignal(cm)

	s.server.Route(
		alaserv.QueueSetting{Name: "config-queue"},
		[]alaserv.ExchangeSetting{{Name: "config-exch", Kind: "direct", RouteKey: "direct-config"}},
		s.configHandler,
	)

	go func() {
		// wait for shutdown signal.
		<-ctx.Done()

		_ = s.server.Close()
	}()

	s.server.Start(ctx)
}

// NewConfigHandler creates a new instance of ConfigHandler.
func NewConfigHandler(l logger.Logger) *ConfigHandler {
	return &ConfigHandler{log: l}
}

// ConfigHandler is an example handler for alaserv function handler.
type ConfigHandler struct {
	log logger.Logger
}

// ServeAMQP is an AMQP handler that print body of the delivery.
func (c *ConfigHandler) ServeAMQP(ctx context.Context, d amqp.Delivery, p alaserv.Publisher) error {
	c.log.Info("received deliv.Body: %s", string(d.Body))
	return nil
}

// stopWhenSignalReceived stops programs when system giving signal to terminate.
func stopWhenSignalReceived(l logger.Logger, cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigCh
	l.Info("%sed", sig.String())
	cancel()
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	serveContext, cancel := context.WithCancel(context.Background())
	l := logger.L

	server := New(l, "amqp://testusr:testusr@localhost:5672/vhost")

	go stopWhenSignalReceived(l, cancel)

	server.Listen(serveContext)

}
