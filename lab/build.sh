#!/bin/sh
# Builds the static binaries of the tzlab image into lab/image/bin.
set -e
cd "$(dirname "$0")/.."
mkdir -p lab/image/bin
CGO_ENABLED=0 go build -trimpath -o lab/image/bin/ ./cmd/tzd ./lab/zlab
