package common

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	BatchMaxAmount int
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

// StartClientLoop Send messages to the client until some time threshold is met or a SIGTERM is received
func (c *Client) StartClientLoop() {
	c.handleSignals()

	// Open the data file to read the bets
	filePath := fmt.Sprintf("./data/agency-%s.csv", c.config.ID)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf(
			"action: file_open | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	defer file.Close()

	fileReader := bufio.NewReader(file)

	// Create the connection to the server only once.
	err = c.createClientSocket()
	if err != nil {
		// Connection failed, error already logged in createClientSocket method
		return
	}
	defer c.conn.Close()

	// TODO: Improve waiting for response
	c.conn.SetReadDeadline(time.Now().Add(100000000 * time.Second))

	idMessage := fmt.Sprintf("HELLO|%s\n", c.config.ID)
	err = c.sendAllMessage(idMessage)
	if err != nil {
		log.Errorf("action: send_hello | result: fail | error: %v", err)
		return
	}

	for {
		select {
		// This case implies a SIGTERM signal has bee received, so we should close gracefully this client
		case <-c.stop:
			log.Infof("action: shutdown | result: in_progress | client_id: %v", c.config.ID)
			return
		default:
			betsBatch, err := c.readBetsBatch(fileReader)
			if err != nil {
				log.Errorf("action: read_batch | result: fail | error: %v", err)
				return
			}

			if len(betsBatch) == 0 {
				log.Infof("action: send_batches | result: success | client_id: %v", c.config.ID)
				err := c.finishSendingAndQueryWinners()
				if err != nil {
					log.Infof("action: query_winners | result: fail | client_id: %v | err: ", c.config.ID, err)
				}
				log.Infof("action: query_winners | result: success | client_id: %v", c.config.ID)

				return
			}

			err = c.sendBetsBatch(betsBatch)
			if err != nil {
				log.Errorf("action: send_batch | result: fail | error: %v", err)
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *Client) readBetsBatch(reader *bufio.Reader) ([]Bet, error) {
	betsBatch := []Bet{}
	currentBatchWeight := 0

	for {
		betData, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return []Bet{}, err
		}

		// The data in the file is separated by comma values, so get fields by splitting by comma
		betFields := strings.Split(strings.TrimSpace(betData), ",")
		if len(betFields) < 5 {
			continue
		}

		betInfo := Bet{
			Nombre:     betFields[0],
			Apellido:   betFields[1],
			Documento:  betFields[2],
			Nacimiento: betFields[3],
			Numero:     betFields[4],
		}

		betsBatch = append(betsBatch, betInfo)

		// Analize replacing this with SerializeBet fn
		betWeight := len(
			fmt.Sprintf("%s|%s|%s|%s|%s\n",
				betInfo.Nombre,
				betInfo.Apellido,
				betInfo.Documento,
				betInfo.Nacimiento,
				betInfo.Numero,
			),
		)
		currentBatchWeight += betWeight

		// Check if the next will surpase the length limit or the size one (this is hipotetically, suposing the next would weight as much as the current)
		if (len(betsBatch)+1 > c.config.BatchMaxAmount) || float64(currentBatchWeight+betWeight)/1024.0 > 8 {
			log.Info("Salgo porque paso el límite, len es %d y weight es %d", len(betsBatch), currentBatchWeight)
			break
		}
	}

	return betsBatch, nil
}

func (c *Client) sendBetsBatch(betsBatch []Bet) error {
	if len(betsBatch) == 0 {
		return nil
	}

	// Send the config ID (agencyId) and the batch length first
	idMessage := fmt.Sprintf("%s|%d\n", c.config.ID, len(betsBatch))
	err := c.sendAllMessage(idMessage)
	if err != nil {
		log.Errorf("action: send_id | result: fail | error: %v", err)
		return err
	}

	// Read initial message's server ack response
	reader := bufio.NewReader(c.conn)
	ack, err := reader.ReadString('\n')
	if err != nil || ack != fmt.Sprintf("OK|%s\n", c.config.ID) {
		log.Errorf("action: receive_initial_ack | result: fail | ack: %v | error: %v", ack, err)
		return err
	}

	message := SerializeBetsBatch(betsBatch)
	err = c.sendAllMessage(message)
	if err != nil {
		log.Errorf("action: send_message | result: fail | error: %v", err)
		return err
	}

	finalAck, err := reader.ReadString('\n')
	if err != nil || strings.Trim(finalAck, "\n") != "OK" {
		log.Errorf("action: receive_final_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	log.Infof("action: apuesta_enviada | result: success | bets_number: %v", len(betsBatch))

	return nil
}

// Example of serializing: "Santiago|Lorca|30904465|1999-03-17|7574\nFacundo Benjamin|Pérez|27469637|1990-12-16|6386\n"
func SerializeBetsBatch(betsBatch []Bet) string {
	lines := []string{}
	for _, bet := range betsBatch {
		line := fmt.Sprintf("%s|%s|%s|%s|%s", bet.Nombre, bet.Apellido, bet.Documento, bet.Nacimiento, bet.Numero)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n") + "\n"
}

func (c *Client) finishSendingAndQueryWinners() error {
	// Send the finish message to the server, that implies all bets have been sent
	finishMessage := fmt.Sprintf("WIN|%s\n", c.config.ID)
	err := c.sendAllMessage(finishMessage)
	if err != nil {
		log.Errorf("action: send_id | result: fail | error: %v", err)
		return err
	}


	// Protocolo de envío de ganadores: Mando desde el servidor inicialmente el ID de la agencia y la cantidad de ganadores
	reader := bufio.NewReader(c.conn)

	initialWinnersMsg, err := reader.ReadString('\n')
	initialMsgFields := strings.Split(initialWinnersMsg, "|")
	if err != nil || initialMsgFields[0] != c.config.ID {
		log.Errorf("action: receive_initial_winners_message | result: fail | client_id: %v | error: %v", initialMsgFields[0], err)
		return err
	}

	winnersAmount, err := strconv.Atoi(strings.Trim(initialMsgFields[1], "\n"))
	if err != nil {
		log.Errorf("action: parse_winners_amount | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// TODO: Should send an ok here?

	// Receiving only the winners DNIs, separated by `|`
	betWinnersDocument, err := reader.ReadString('\n')
	if err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	if winnersAmount > 0 {
		totalWinners := strings.Split(betWinnersDocument, "|")
		if winnersAmount != len(totalWinners) {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: expected winners amount does not match the winners read", c.config.ID)
			return err
		}		
	}

	log.Info("action: consulta_ganadores | result: success | cant_ganadores: ", winnersAmount)

	return nil
}

// Sends all message, preventing short writes, returning error in case an error happened while sending it
func (c *Client) sendAllMessage(message string) error {
	for len(message) > 0 {
		number_bytes_sent, err := c.conn.Write([]byte(message))
		if err != nil {
			return err
		}

		message = message[number_bytes_sent:]
	}

	return nil
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
