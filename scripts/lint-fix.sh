#!/bin/sh
# Auto-fix linting issues

set -e

echo "=== Auto-fixing linting issues ==="

echo ""
echo "1. Removing unused imports with autoflake..."
python3 -m autoflake --ignore-init-module-imports \
    --remove-unused-variables --remove-all-unused-imports \
    --in-place --recursive .

echo ""
echo "2. Sorting imports with isort..."
python3 -m isort . --profile black

echo ""
echo "3. Formatting code with black..."
python3 -m black .

echo ""
echo "=== Auto-fix complete ==="

