#!/bin/sh
# Linting script with static checks for unused code and imports

set -e  # Exit on any error

echo "=== Running static code analysis ==="

# Check for unused imports using autoflake (dry-run first to detect issues)
echo ""
echo "1. Checking for unused imports with autoflake..."
if ! python3 -m autoflake --check --recursive --remove-all-unused-imports --remove-unused-variables . 2>&1 | grep -q "would be removed"; then
    echo "   ✓ No unused imports found"
else
    echo "   ✗ Unused imports detected. Run 'make lint-fix' to auto-fix."
    python3 -m autoflake --check --recursive --remove-all-unused-imports --remove-unused-variables . 2>&1 | head -20
    exit 1
fi

# Check for unused code using vulture (if available)
if command -v vulture >/dev/null 2>&1; then
    echo ""
    echo "2. Checking for unused code with vulture..."
    vulture . --min-confidence 80 --exclude venv,__pycache__,.git,etl/__pycache__,sync/__pycache__,queries/__pycache__ || true
    echo "   ✓ Vulture check completed (warnings are non-blocking)"
else
    echo ""
    echo "2. Skipping vulture (not installed). Install with: pip install vulture"
fi

# Format code with isort
echo ""
echo "3. Checking import sorting with isort..."
if ! python3 -m isort --check-only --profile black .; then
    echo "   ✗ Import sorting issues found. Run 'make lint-fix' to auto-fix."
    exit 1
else
    echo "   ✓ Import sorting is correct"
fi

# Format code with black
echo ""
echo "4. Checking code formatting with black..."
if ! python3 -m black --check .; then
    echo "   ✗ Code formatting issues found. Run 'make lint-fix' to auto-fix."
    exit 1
else
    echo "   ✓ Code formatting is correct"
fi

# Type checking with mypy
echo ""
echo "5. Running type checking with mypy..."
python3 -m mypy . || {
    echo "   ⚠ Type checking found issues (non-blocking)"
}

# Docstring style with pydocstyle
echo ""
echo "6. Checking docstring style with pydocstyle..."
python3 -m pydocstyle . --convention=google || {
    echo "   ⚠ Docstring style issues found (non-blocking)"
}

echo ""
echo "=== Static analysis complete ==="
echo "✓ All critical checks passed"
