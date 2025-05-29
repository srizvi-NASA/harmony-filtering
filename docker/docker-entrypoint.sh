#!/usr/bin/env bash
set -euo pipefail

# If first arg starts with a dash, treat it as filter args
if [[ "${1:0:1}" = "-" ]]; then
  exec python -m harmony_filtering_service.adapter "$@"
fi

# Otherwise pull off the “mode” and dispatch
mode="$1"; shift

case "$mode" in
  filter)
    exec python -m harmony_filtering_service.adapter "$@" ;;
  dev)
    exec python -m harmony_filtering_service.adapter --dev "$@" ;;
  *)
    exec "$mode" "$@" ;;
esac
