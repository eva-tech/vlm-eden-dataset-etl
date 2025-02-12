#!/bin/sh
{
  echo "Executing autoflake"
  python3 -m autoflake --ignore-init-module-imports \
    --remove-unused-variables --remove-all-unused-imports \
    --in-place --recursive .

  echo "Executing isort"
  python3 -m isort . --profile black

  echo "Executing black"
  python3 -m black .

  echo "Executing mypy"
  python3 -m mypy .

  echo "Executing pydocstyle"
  python3 -m pydocstyle .

  echo "success"
  exit 0
} || { # catch
  exit 1
}