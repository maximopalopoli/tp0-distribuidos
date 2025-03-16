#!/bin/bash

# Recibo el nombre del archivo de salida ($1) y la cantidad de clientes que debería tener ($2)

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
" > ${fileName}
