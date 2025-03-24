package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
	stop   chan struct{}
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
		stop:   make(chan struct{}),
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		// return err? anyway it's going to happen, but in that case should not return error in this func
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met or a SIGTERM is received
func (c *Client) StartClientLoop() {
	c.handleSignals()

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		select {
		// This case implies a SIGTERM signal has bee received, so we should close gracefully this client
		case <-c.stop:
			log.Infof("action: shutdown | result: in_progress | client_id: %v", c.config.ID)
			return
		default:
			// Create the connection the server in every loop iteration. Send an
			c.createClientSocket()

			// TODO: Modify the send to avoid short-write
			fmt.Fprintf(
				c.conn,
				"[CLIENT %v] Message N°%v\n",
				c.config.ID,
				msgID,
			)
			msg, err := bufio.NewReader(c.conn).ReadString('\n')
			c.conn.Close()

			if err != nil {
				log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
				c.config.ID,
				msg,
			)

			// Wait a time between sending one message and the next one
			time.Sleep(c.config.LoopPeriod)
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}

// handleSignals Captures SIGTERM signal and stops the client calling StopClient method.
func (c *Client) handleSignals() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	// This goroutine will capture the termination signal
	go func() {
		sig := <-sigChan
		log.Infof("action: received_signal | signal: %v | client_id: %v", sig, c.config.ID)
		c.StopClient()
	}()
}

// StopClient Stops the client gracefully, making the main loop finish via closing the stop chan
func (c *Client) StopClient() {
	close(c.stop)
	log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
}
