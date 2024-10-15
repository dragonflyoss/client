#!/bin/bash

# Create the directory used for storing certs.
mkdir -p certs
cd certs

# Generate CA private key and self-signed cert.
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 1024 -out ca.crt -subj "/C=CN/ST=Beijing/L=Beijing/O=Test CA/OU=IT/CN=Test CA"

# Generate another CA private key and self-signed cert.
openssl genrsa -out wrong-ca.key 2048
openssl req -x509 -new -nodes -key wrong-ca.key -sha256 -days 1024 -out wrong-ca.crt -subj "/C=CN/ST=Beijing/L=Beijing/O=Wrong CA/OU=IT/CN=Wrong CA"

# Generate OpenSSL config file with SAN extention.
cat >san.cnf <<EOL
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
[req_distinguished_name]
[ v3_req ]
subjectAltName = @alt_names
[ alt_names ]
DNS.1 = localhost
EOL

# Generate server private key and CSR.
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/C=CN/ST=Beijing/L=Beijing/O=Test Server/OU=IT/CN=localhost" -config san.cnf

# Sign server CSR by using CA with SAN extension.
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 500 -sha256 -extensions v3_req -extfile san.cnf

# Convert crt to pem.
openssl x509 -in server.crt -out server.pem -outform PEM

rm *.csr
