#!/bin/sh

set -e

# Source the keystore utility functions
. ./keystore.sh

NICKNAME=$1
WASM_FILE=$2

# Validate input
if [ $# -ne 2 ]; then
    echo "Usage: $0 <nickname> <wasm_file_path>" >&2
    exit 1
fi

# Extract the address using functions
ADDRESS=$(get_address_for_nick "$NICKNAME")

if [ -z "$ADDRESS" ]; then
    echo "Failed to extract address from keystore" >&2
    exit 1
fi

# Check if WASM file exists
if [ ! -f "$WASM_FILE" ]; then
    echo "Error: WASM file '$WASM_FILE' not found" >&2
    exit 1
fi

echo "Storing WASM code from $WASM_FILE for $NICKNAME"
# Execute the command
echo canopy admin tx-store-code "$ADDRESS" "$WASM_FILE" --fee=50000 --password=test
canopy admin tx-store-code "$ADDRESS" "$WASM_FILE" --fee=50000 --password=test
