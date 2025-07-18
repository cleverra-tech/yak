#!/bin/bash

# Generate self-signed certificates for testing SSL/TLS

set -e

CERT_DIR="./certs"
mkdir -p "$CERT_DIR"

echo "Generating self-signed certificates for testing..."

# Generate private key
openssl genrsa -out "$CERT_DIR/key.pem" 2048

# Generate certificate signing request
openssl req -new -key "$CERT_DIR/key.pem" -out "$CERT_DIR/cert.csr" -subj "/C=US/ST=Test/L=Test/O=Yak AMQP Broker/CN=localhost"

# Generate self-signed certificate
openssl x509 -req -in "$CERT_DIR/cert.csr" -signkey "$CERT_DIR/key.pem" -out "$CERT_DIR/cert.pem" -days 365

# Clean up CSR
rm "$CERT_DIR/cert.csr"

echo "Generated certificates:"
echo "  Private key: $CERT_DIR/key.pem"
echo "  Certificate: $CERT_DIR/cert.pem"
echo ""
echo "To use these certificates, update your config.json:"
echo "  \"ssl\": {"
echo "    \"enabled\": true,"
echo "    \"cert_file\": \"$CERT_DIR/cert.pem\","
echo "    \"key_file\": \"$CERT_DIR/key.pem\""
echo "  }"