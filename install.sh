#!/bin/bash
set -e

# Install yarli to ~/.local/bin
# This follows standard Unix conventions and ~/.local/bin is often in PATH

INSTALL_DIR="$HOME/.local/bin"
BINARY_NAME="yarli"

echo "Building yarli in release mode..."
cargo build --release -p yarli-cli --bin yarli

echo "Creating install directory: $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"

echo "Installing yarli to $INSTALL_DIR/$BINARY_NAME"
# Avoid "Text file busy" by doing an atomic replace.
TMP_PATH="$INSTALL_DIR/${BINARY_NAME}.new.$$"
cp target/release/yarli "$TMP_PATH"
chmod +x "$TMP_PATH"
mv -f "$TMP_PATH" "$INSTALL_DIR/$BINARY_NAME"

echo ""
echo "yarli installed successfully."
echo ""
echo "Installation path: $INSTALL_DIR/$BINARY_NAME"
echo ""

# Check if ~/.local/bin is in PATH
if [[ ":$PATH:" == *":$HOME/.local/bin:"* ]]; then
    echo "$HOME/.local/bin is already in your PATH."
    echo ""
    echo "You can now run: yarli --help"
else
    echo "$HOME/.local/bin is NOT in your PATH."
    echo ""
    echo "Add this line to your ~/.bashrc or ~/.zshrc:"
    echo ""
    echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
    echo "Then run: source ~/.bashrc (or source ~/.zshrc)"
    echo ""
    echo "Or run yarli with full path: $INSTALL_DIR/$BINARY_NAME"
fi

echo ""
echo "To verify installation:"
echo "  $INSTALL_DIR/$BINARY_NAME --version"
