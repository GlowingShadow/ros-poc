#!/usr/bin/env bash
#
# run.sh — build and start the 4-container ROS2 pipeline (manager + A/B/C).
#
# Usage:
#   ./scripts/run.sh [copy|zero_copy] [options]
#   ./scripts/run.sh down            # stop and remove the stack
#
# Options:
#   -d, --detach     run in the background (default: foreground, Ctrl-C to stop)
#       --no-build   skip the image build (use the existing ros2-poc-app:latest)
#       --rebuild    force a colcon rebuild inside the containers (FORCE_REBUILD=1)
#   -h, --help       show this help
#
# The transfer mode can also be set via the MODE env var; the positional
# argument wins if both are given.
#
# Examples:
#   ./scripts/run.sh                 # copy mode, build, foreground
#   ./scripts/run.sh zero_copy       # zero-copy mode
#   ./scripts/run.sh copy -d         # copy mode, detached
#   ./scripts/run.sh down            # tear everything down
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.ros2.yml"

usage() {
  cat <<'EOF'
run.sh — build and start the 4-container ROS2 pipeline (manager + A/B/C).

Usage:
  ./scripts/run.sh [copy|zero_copy] [options]
  ./scripts/run.sh down                 stop and remove the stack

Options:
  -d, --detach     run in the background (default: foreground, Ctrl-C to stop)
      --no-build   skip the image build (reuse the existing ros2-poc-app:latest)
      --rebuild    force a colcon rebuild inside the containers (FORCE_REBUILD=1)
  -h, --help       show this help

The transfer mode can also be set via the MODE env var; the positional
argument wins if both are given.

Examples:
  ./scripts/run.sh                 copy mode, build, foreground
  ./scripts/run.sh zero_copy       zero-copy mode
  ./scripts/run.sh copy -d         copy mode, detached
  ./scripts/run.sh down            tear everything down
EOF
}

# --- handle help early, before any docker checks ---
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
  esac
done

# --- pick the compose command (v2 plugin preferred, v1 fallback) ---
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "error: neither 'docker compose' nor 'docker-compose' is available." >&2
  echo "       Are you inside the devcontainer with docker-in-docker enabled," >&2
  echo "       and did the nested Docker daemon start? Try: docker version" >&2
  exit 1
fi

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "error: $COMPOSE_FILE not found." >&2
  exit 1
fi

# --- parse args ---
MODE="${MODE:-copy}"
BUILD=1
DETACH=0
REBUILD=0

# 'down' subcommand: tear the stack down and exit.
if [ "${1:-}" = "down" ]; then
  echo ">> stopping pipeline stack"
  exec "${COMPOSE[@]}" -f "$COMPOSE_FILE" down
fi

while [ $# -gt 0 ]; do
  case "$1" in
    copy|zero_copy) MODE="$1" ;;
    -d|--detach)    DETACH=1 ;;
    --no-build)     BUILD=0 ;;
    --rebuild)      REBUILD=1 ;;
    -h|--help)      usage; exit 0 ;;
    *) echo "error: unknown argument '$1'" >&2; echo; usage; exit 1 ;;
  esac
  shift
done

if [ "$MODE" != "copy" ] && [ "$MODE" != "zero_copy" ]; then
  echo "error: MODE must be 'copy' or 'zero_copy', got '$MODE'" >&2
  exit 1
fi

# --- verify the Docker daemon is reachable ---
if ! docker version >/dev/null 2>&1; then
  echo "error: cannot talk to the Docker daemon (docker version failed)." >&2
  echo "       Inside the devcontainer, the docker-in-docker daemon may still be" >&2
  echo "       starting up — wait a few seconds and retry." >&2
  exit 1
fi

up_args=(up)
[ "$BUILD" -eq 1 ] && up_args+=(--build)
[ "$DETACH" -eq 1 ] && up_args+=(-d)

echo ">> mode=$MODE  build=$BUILD  detach=$DETACH  rebuild=$REBUILD"
echo ">> outputs will appear in: $REPO_ROOT/output_frames/  (frame_N_${MODE}.png)"
[ "$DETACH" -eq 1 ] && echo ">> follow logs with: ${COMPOSE[*]} -f docker-compose.ros2.yml logs -f"

export MODE
[ "$REBUILD" -eq 1 ] && export FORCE_REBUILD=1

cd "$REPO_ROOT"
exec "${COMPOSE[@]}" -f "$COMPOSE_FILE" "${up_args[@]}"
