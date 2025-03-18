#!/bin/bash

# Recibo el nombre del archivo de salida ($1) y la cantidad de clientes que debería tener ($2)

# TODO: verificar de alguna forma que se hayan recibido parámetros
fileName=$1
amountOfClients=$2

echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"

echo "name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini
" > ${fileName}

for ((i=1; i<=amountOfClients; i++)); do
    echo "  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=$i
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    volumes:
      - ./client/config.yaml:/config.yaml
    depends_on:
      - server
" >> ${fileName}
done

echo "networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
" >> ${fileName}
