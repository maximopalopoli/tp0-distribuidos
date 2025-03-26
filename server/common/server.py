import socket
import logging
import signal

from common.utils import Bet, store_bets

def deserialize_bet(message: str):
    fields = message.strip().split('|')
    return {
        "nombre": fields[0],
        "apellido": fields[1],
        "dni": fields[2],
        "nacimiento": fields[3],
        "numero": fields[4],
    }

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.is_running = True

        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while self.is_running:
            try:
                client_sock = self.__accept_new_connection()
                # Dont handle the connection if accept failed
                if client_sock:
                    self.__handle_client_connection(client_sock)
            except OSError as e:
                if not self.is_running:
                    logging.info("action: shutdown | result: server_stopped")
                    break 
                else:
                    logging.error(f"action: accept_connection | result: fail | error: {e}") 

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            # Start receiving the agency id
            init_msg_data = self.receive_data(client_sock)
            if not init_msg_data:
                logging.error(f'action: receive_message | result: fail | error: error while reading agency id')
            addr = client_sock.getpeername()

            init_fields = init_msg_data.strip().split('|')
            agency_id = init_fields[0]
            bets_amount = int(init_fields[1])
            logging.info(f'action: receive_id | result: success | ip: {addr[0]} | agency_id: {agency_id}| bets_amount: {bets_amount}')

            # Send initial ACK with agency id
            ack_message = f"OK|{agency_id}\n".encode("utf-8")
            client_sock.sendall(ack_message)

            received_bets = []
            # Receive and deserialize the rest of bet information
            for i in range(bets_amount):
                # Receive raw data from socket
                bet_message = self.receive_data(client_sock)
                bet_data = deserialize_bet(bet_message)
                if len(bet_data) < 5:
                    logging.error(f"action: apuesta_recibida | result: fail | cantidad: ${i}")
                    break
            
                # Create and store Bet
                bet = Bet(agency_id, bet_data["nombre"], bet_data["apellido"], bet_data["dni"], bet_data["nacimiento"], bet_data["numero"])            
                received_bets.append(bet)

            store_bets(received_bets)
            
            logging.info(f"action: apuesta_recibida | result: success | cantidad: ${bets_amount}")

            # Send final ACK including dni and bet number
            response = f"OK\n".encode("utf-8")
            client_sock.sendall(response)

        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            logging.info("action: close_socket_client | result: success")

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def receive_data(self, sock):
        """Receives data until find a '\n', and returns the data read until that moment."""
        received_msg = b""
        while True:
            data = sock.recv(1)
            if not data:
                break
            received_msg += data
            if data == b"\n":
                break
        return received_msg.decode("utf-8").strip()

    def _handle_shutdown(self, signum, _):
        """
        Handles SIGTERM signal to perform a graceful shutdown in case that signal is received.

        Sets should_shutdown as true and closes server socket, also logging the shutdown.
        """
        logging.info(f"action: shutdown | signal: {signum} | result: in_progress")
        self.is_running = False
        self._server_socket.close()
        logging.info("action: shutdown | result: success")
        