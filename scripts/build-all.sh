#!/bin/bash
# Build script for Artemis with embedded UI

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Building Artemis with Embedded UI ==="
echo ""

# Step 1: Build UI
echo "Step 1/2: Building UI..."
bash "$SCRIPT_DIR/build-ui.sh"
echo ""

# Step 2: Build Go binary with embedded assets
echo "Step 2/2: Building Artemis binary with embedded UI..."
cd "$ROOT_DIR"

# Build with builtinassets tag to embed the UI
go build -tags builtinassets -o artemis ./cmd/artemis

echo ""
echo "=== Build Complete ==="
echo "Binary: ./artemis"
echo "Run with: ./artemis"
