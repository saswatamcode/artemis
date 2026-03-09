#!/bin/bash
# Build script for Artemis UI

set -e

echo "Building Artemis UI..."

# Navigate to UI directory
cd "$(dirname "$0")/../ui"

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "Installing dependencies..."
    npm install
fi

# Build the React app
echo "Building React app..."
npm run build

# Copy dist to pkg/ui/dist for embedding
echo "Copying build artifacts to pkg/ui/dist..."
rm -rf ../pkg/ui/dist
cp -r dist ../pkg/ui/dist

echo "UI build complete!"
echo "Build artifacts copied to pkg/ui/dist"
