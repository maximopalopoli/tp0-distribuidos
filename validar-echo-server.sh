#!/bin/bash

# Levanto los contenedores en segundo plano y espero un tiempo para que el servidor esté listo
make docker-compose-up
sleep 5

mensajeEnviado="validar-echo-server"
server="server"
puerto=12345

# Hacer netcat usando el cliente validador y guardar el resultado en una variable
mensajeRecibido=$(docker run --rm --network=tp0_testing_net busybox sh -c "echo \"$mensajeEnviado\" | nc $server $puerto")

# Si el la respuesta es igual al mensaje enviado, entonces el flujo fue el correcto
if [ "$mensajeRecibido" = "$mensajeEnviado" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi

# Cerrar los contenedores después de la prueba
make docker-compose-down
