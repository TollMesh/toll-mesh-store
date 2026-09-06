#!/usr/bin/env bash
#
# generate-cluster-ca.sh -- bootstrap a self-signed CA and per-node leaf
# certificates for a TollMeshCache cluster's -tls-cert/-tls-key/-tls-ca
# flags (see docs/operations.md#tls-and-certificates).
#
# This is a minimal starting point for a small deployment or local
# testing, NOT a substitute for a real PKI (Vault, cert-manager, an
# internal CA with revocation and audit trails) in an environment that
# needs real certificate lifecycle management. It has no revocation
# story at all: if a node's key leaks, your only remedy is regenerating
# the whole CA and every leaf cert.
#
# Usage:
#   ./generate-cluster-ca.sh <output-dir> <node-address> [<node-address> ...]
#
# Each <node-address> is whatever address peers use to reach that node
# (an IP or hostname) -- it becomes that node's certificate's Subject
# Alternative Name. Every node in the cluster gets ITS OWN cert/key
# signed by the one shared CA; all nodes are given the same ca-cert.pem
# to pass as -tls-ca.
#
# Example, matching this session's own live-verification commands:
#   ./generate-cluster-ca.sh ./certs 127.0.0.1
#   ./generate-cluster-ca.sh ./certs node1.internal node2.internal node3.internal
#
# Output layout:
#   <output-dir>/ca-cert.pem            -- give to every node as -tls-ca
#   <output-dir>/<address>/node-cert.pem -- that node's -tls-cert
#   <output-dir>/<address>/node-key.pem  -- that node's -tls-key (keep private)
#
# Certificates are valid for 397 days (the maximum most browsers/clients
# accept for a leaf cert as of this writing) for nodes, and 10 years for
# the CA. Re-run this script (it's idempotent per fresh output-dir) before
# expiry; there is no separate renewal command, since CertReloader picks
# up a freshly-generated file at the same path automatically (see
# docs/operations.md).

set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <output-dir> <node-address> [<node-address> ...]" >&2
  exit 1
fi

OUTPUT_DIR="$1"
shift
NODE_ADDRESSES=("$@")

if ! command -v openssl >/dev/null 2>&1; then
  echo "error: openssl is required but not found on PATH" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

if [ -f ca-cert.pem ] || [ -f ca-key.pem ]; then
  echo "error: $OUTPUT_DIR/ca-cert.pem or ca-key.pem already exists -- refusing to overwrite an existing CA." >&2
  echo "       Use a fresh output directory, or remove the existing CA files first if you really intend to replace them" >&2
  echo "       (replacing the CA invalidates every leaf cert it previously signed across the whole cluster)." >&2
  exit 1
fi

echo "Generating cluster CA in $OUTPUT_DIR/ca-cert.pem (valid 10 years)..."
openssl req -x509 -newkey rsa:2048 -keyout ca-key.pem -out ca-cert.pem \
  -days 3650 -nodes -subj "/CN=tollmeshcache-cluster-ca" 2>/dev/null
chmod 600 ca-key.pem

for ADDR in "${NODE_ADDRESSES[@]}"; do
  NODE_DIR="$ADDR"
  mkdir -p "$NODE_DIR"

  # Detect whether the address is an IP (goes in IP.1) or a hostname
  # (goes in DNS.1) -- both are needed since the certificate must match
  # however peers actually dial this node.
  if [[ "$ADDR" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    SAN_LINE="IP.1 = $ADDR"
  else
    SAN_LINE="DNS.1 = $ADDR"
  fi

  SAN_CONF="$NODE_DIR/san.cnf"
  cat > "$SAN_CONF" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
[req_distinguished_name]
[v3_req]
subjectAltName = @alt_names
[alt_names]
$SAN_LINE
EOF

  echo "Generating certificate for $ADDR in $NODE_DIR/node-cert.pem (valid 397 days)..."
  openssl req -newkey rsa:2048 -keyout "$NODE_DIR/node-key.pem" -out "$NODE_DIR/node-req.pem" \
    -nodes -subj "/CN=$ADDR" -config "$SAN_CONF" -reqexts v3_req 2>/dev/null
  openssl x509 -req -in "$NODE_DIR/node-req.pem" -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -out "$NODE_DIR/node-cert.pem" -days 397 -extfile "$SAN_CONF" -extensions v3_req 2>/dev/null

  chmod 600 "$NODE_DIR/node-key.pem"
  rm -f "$NODE_DIR/node-req.pem" "$SAN_CONF"
done

echo
echo "Done. For each node, pass:"
echo "  -tls-cert $OUTPUT_DIR/<address>/node-cert.pem"
echo "  -tls-key  $OUTPUT_DIR/<address>/node-key.pem"
echo "  -tls-ca   $OUTPUT_DIR/ca-cert.pem   (the SAME file on every node)"
echo
echo "Keep every *-key.pem private -- distribute only to the node it belongs to."
