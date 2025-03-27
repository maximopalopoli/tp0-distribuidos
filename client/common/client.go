package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
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

type Bet struct {
	Nombre     string
	Apellido   string
	Documento  string
	Nacimiento string
	Numero     string
	Agencia    string
}

// Example of serializing: "Santiago|Lorca|30904465|1999-03-17|7574\n"
func SerializeBet(nombre, apellido, dni, nacimiento, numero string) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s\n", nombre, apellido, dni, nacimiento, numero)
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

// CreateClientSocket Initializes client socket. In case of failure, error is printed in
// stdout/stderr and exit 1 is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) getBetFromEnvironment() Bet {
	name := os.Getenv("NOMBRE")
	lastname := os.Getenv("APELLIDO")
	document := os.Getenv("DOCUMENTO")
	birthdate := os.Getenv("NACIMIENTO")
	number := os.Getenv("NUMERO")

	return Bet{
		name,
		lastname,
		document,
		birthdate,
		number,
		c.config.ID,
	}
}

// StartClientLoop Send messages to the client until some time threshold is met or a SIGTERM is received
func (c *Client) StartClientLoop() {
	c.handleSignals()

	betInfo := c.getBetFromEnvironment()
	
	// There is an autoincremental msgID to identify every message sent, messages are sent if the threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		select {
		// This case implies a SIGTERM signal has bee received, so we should close gracefully this client
		case <-c.stop:
			log.Infof("action: shutdown | result: in_progress | client_id: %v", c.config.ID)
			return
		default:
			// Create the connection the server in every loop iteration. Send an
			err := c.createClientSocket()
			if err != nil {
				// Connection failed, error already logged in createClientSocket method
				return
			}
			defer c.conn.Close()

			// Send the config ID (agencyId) first
			idMessage := fmt.Sprintf("%s\n", c.config.ID)
			_, err = c.conn.Write([]byte(idMessage))
			if err != nil {
				log.Errorf("action: send_id | result: fail | error: %v", err)
				return
			}

			// Read initial message's server ack response
			reader := bufio.NewReader(c.conn)
			ack, err := reader.ReadString('\n')
			if err != nil || ack != fmt.Sprintf("OK|%s\n", c.config.ID) {
				log.Errorf("action: receive_initial_ack | result: fail | error: %v", err)
				return
			}
			log.Infof("La info de nacimiento es %v", betInfo.Nacimiento)

			// Send the rest of the bet information in the protocol format
			message := SerializeBet(betInfo.Nombre, betInfo.Apellido, betInfo.Documento, betInfo.Nacimiento, betInfo.Numero)
			_, err = c.conn.Write([]byte(message))
			if err != nil {
				log.Errorf("action: send_message | result: fail | error: %v", err)
				return
			}

			// Read final ack, that includes an OK, the document and the bet number
			finalAck, err := reader.ReadString('\n')
			if err != nil {
				log.Errorf("action: receive_final_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
				return
			}

			ackFields := strings.Split(strings.TrimSpace(finalAck), "|")
			documento := ackFields[1]
			numero := ackFields[2]

			log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", documento, numero)
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
