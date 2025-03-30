import select
import socket
import logging
import signal
import multiprocessing

from common.utils import Bet, has_won, load_bets, store_bets

def deserialize_bet(message: str):
    fields = message.strip().split('|')
    return {
        "nombre": fields[0],
        "apellido": fields[1],
        "dni": fields[2],
        "nacimiento": fields[3],
        "numero": fields[4],
    }

def handle_client_connection(client_sock, active_agencies, finished, finished_lock, write_bets_lock):
    """
    Read message from a specific client socket and closes the socket

    If a problem arises in the communication with the client, the
    client socket will also be closed
    """
    try:
        # Receive client hello message
        hello_msg_data = receive_data_with_length(client_sock, 8)
        if not hello_msg_data:
            logging.error(f'action: receive_hello_message | result: fail | error: error while reading hello message')
            return
        addr = client_sock.getpeername()

        hello_fields = hello_msg_data.strip().split('|')
        if hello_fields[0] != "HELLO":
            logging.error(f'action: parse_hello_message | result: fail | error: First connection message should start with hello')
            return

        agency_id = int(hello_fields[1])
        if not agency_id in active_agencies:
            active_agencies[agency_id] = client_sock
            
        addr = client_sock.getpeername()
        logging.info(f'action: receive_hello_message | result: success | ip: {addr[0]} | agency_id: {agency_id}')
            
        # Now we are receiving batches of bets, repeating the logic for each batch, unti we receive the WIN message
        while True:
            # Start receiving the agency id
            init_msg_data = receive_data_with_length(client_sock, 7)
            if not init_msg_data:
                logging.error(f'action: receive_message | result: fail | error: error while reading agency id')

            init_fields = init_msg_data.strip().split('|')
            if init_fields[0] == "WIN":
                with finished_lock:
                    finished.value += 1
                break

            agency_id = int(init_fields[0])
            batch_len = int(init_fields[1])
            logging.info(f'action: receive_id | result: success | ip: {addr[0]} | agency_id: {agency_id}| batch_len: {batch_len}')

            # Send initial ACK with agency id
            ack_message = f"OK|{agency_id}\n".encode("utf-8")
            client_sock.sendall(ack_message)

            received_bets = []

            bet_message = receive_data_with_length(client_sock, batch_len)
            bets = bet_message.strip().split('\n')

            # Receive and deserialize the rest of bet information
            for new_bet in bets:
                bet_data = deserialize_bet(new_bet)
                if len(bet_data) < 5:
                    logging.error(f"action: apuesta_recibida | result: fail | cantidad: {len(bet_data)}")
                    return
                
                # Create and store Bet
                bet = Bet(agency_id, bet_data["nombre"], bet_data["apellido"], bet_data["dni"], bet_data["nacimiento"], bet_data["numero"])            
                received_bets.append(bet)

            with write_bets_lock:
                store_bets(received_bets)
                
            logging.info(f"action: apuesta_recibida | result: success | agency_id: {agency_id}| cantidad: {len(received_bets)}")

            # Send final ACK including dni and bet number
            response = f"OK\n".encode("utf-8")
            client_sock.sendall(response)

    except OSError as e:
        logging.error(f"action: receive_message | result: fail | error: {e}")
    finally:
        if agency_id not in active_agencies:
            client_sock.close()
            logging.info("action: close_socket_client | result: success")

def receive_data_with_length(sock, length):
    """Receives <length> bytes from socket, and return a string with that data."""
    buffer = bytearray()
    while len(buffer) < length:
        chunk = sock.recv(length - len(buffer))
        if not chunk:
            raise ConnectionError("Connection closed unexpectedly")
        buffer.extend(chunk)
    return buffer.decode("utf-8").strip()

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.is_running = True

        self.active_agencies = multiprocessing.Manager().dict()

        self.finished = multiprocessing.Value('i', 0)
        self.finished_lock = multiprocessing.Lock()

        self.write_bets_lock = multiprocessing.Lock()

        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        processes = []
        while self.is_running:
            try:
                ready_to_read, _, _ = select.select([self._server_socket], [], [], 0.5)
                if ready_to_read:
                    client_sock = self.__accept_new_connection()
                    if client_sock:
                        p = multiprocessing.Process(target=handle_client_connection, args=(client_sock, self.active_agencies, self.finished, self.finished_lock, self.write_bets_lock))
                        p.start()
                        processes.append(p)
                if self.finished.value == len(self.active_agencies) and len(ready_to_read) == 0:
                    self.send_winners()
                    return

            except OSError as e:
                if not self.is_running:
                    logging.info("action: shutdown | result: server_stopped")
                    break 
                else:
                    logging.error(f"action: accept_connection | result: fail | error: {e}") 
        for p in processes:
            p.join()

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

    def send_winners(self):
        # Get the winners, then filter per agency
        winners: list[Bet] = []

        bets = load_bets()
        for bet in bets:
            if has_won(bet):
                winners.append(bet)
        
        logging.info('action: sorteo | result: success')

        for curr_agency in range(1,1+self.finished.value): # ids of agencies (from 1 to 5)
            filtered_winners = [bet for bet in winners if bet.agency == curr_agency]

            client_socket = self.active_agencies[curr_agency]

            initial_winner_msg = (str(curr_agency) + "|" + str(len(filtered_winners)) + "\n").encode("utf-8")
            client_socket.sendall(initial_winner_msg)

            winner_documents_str = ("|".join(bet.document for bet in filtered_winners) + "\n").encode("utf-8")
            client_socket.sendall(winner_documents_str)

        logging.info('action: envio_respuestas | result: success')

    def _handle_shutdown(self, signum, _):
        """
        Handles SIGTERM signal to perform a graceful shutdown in case that signal is received.

        Sets should_shutdown as true and closes server socket, also logging the shutdown.
        """
        logging.info(f"action: shutdown | signal: {signum} | result: in_progress")
        self.is_running = False
        self._server_socket.close()
        logging.info("action: shutdown | result: success")
        