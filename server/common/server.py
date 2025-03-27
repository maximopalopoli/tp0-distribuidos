import socket
import logging
import signal


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
            # TODO: Modify the receive to avoid short-reads
            msg = client_sock.recv(1024).rstrip().decode('utf-8')
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            # TODO: Modify the send to avoid short-writes
            client_sock.send("{}\n".format(msg).encode('utf-8'))
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

    def _handle_shutdown(self, signum, _):
        """
        Handles SIGTERM signal to perform a graceful shutdown in case that signal is received.

        Sets should_shutdown as true and closes server socket, also logging the shutdown.
        """
        logging.info(f"action: shutdown | signal: {signum} | result: in_progress")
        self.is_running = False
        self._server_socket.close()
        logging.info("action: shutdown | result: success")
        