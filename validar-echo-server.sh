#!/bin/bash

# Verificar el correcto funcionamiento del servidor utilizando el comando netcat para interactuar con el mismo.
# Como es un echo server, se debe enviar un mensaje al servidor y esperar recibir el mismo mensaje enviado.

mensajeEnviado="validar-echo-server"

# Envio el comando con netcat

# Verifico que lo recibido sea igual a lo enviado

# If equal `action: test_echo_server | result: success`, else `action: test_echo_server | result: fail`

# Constraints:
# Netcat no debe ser instalado en la máquina host
# No se pueden exponer puertos del servidor para realizar la comunicación (hint: docker network).
