#!/usr/bin/env bash

set -eo pipefail

APP_NAME=influx-import
MAIN=main.go
ARCH=amd64
OS=(
    darwin
    linux
    windows
)
BUILD_DIR=build

function GO_BUILD() {
    executable=${APP_NAME}-${GOOS}-${GOARCH}
    if [ $GOOS == "windows" ]; then
        executable=${APP_NAME}-${GOOS}-${GOARCH}.exe
    fi
    go build -ldflags="-s -w" -o $BUILD_DIR/$executable $MAIN
    echo "  $executable done."
    if hash upx 2>/dev/null; then
        echo "  Compressing binary size using UPX for $executable"
        upx --ultra-brute $BUILD_DIR/$executable
        echo "  Compressed for $executable"
    else
        echo "  No UPX installed, binary would not be comporessed"
        if [ `uname` == "Darwin" ]; then
        echo "  Install UPX using brew: \`brew install upx\`"
        fi
    fi
}

# Cleanup build
rm -rf $BUILD_DIR

# Build exe
echo "Building executables..."
for os in "${OS[@]}"; do
    GOOS=$os GOARCH=$ARCH GO_BUILD
done
echo "All executables built."
echo ""

# Package all exe
echo "Packaging executables and configuration file..."
zip -r $BUILD_DIR/influx-import.zip $BUILD_DIR > /dev/null
echo ""

echo "All done."
