#!/usr/bin/env bash
#
# Start the soupcpy agent.
#
# All paths in config/config.yml (templates, db) are relative, so the binary
# must run from this crate directory; the script cd's there for you.
#
# Usage:
#   ./start.sh                 # debug build
#   ./start.sh --release       # optimised build
#   ./start.sh -- --any --args # forward args to the binary
#
# Environment overrides:
#   RUST_LOG             tracing filter          (default: info)
#   CONTROLLER_ENDPOINT  gateway /ws/controller  (default: config.yml)
#   AGENT_UDID           this agent's id         (default: config.yml)
#
# The HTTP/WS listener is hardcoded to 0.0.0.0:3001 in src/main.rs.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

PROFILE_ARGS=()
BIN_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)
            PROFILE_ARGS+=("--release")
            shift
            ;;
        --)
            shift
            BIN_ARGS+=("$@")
            break
            ;;
        -h|--help)
            sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "start.sh: unknown option '$1' (use -- to forward args)" >&2
            exit 2
            ;;
    esac
done

if ! command -v cargo >/dev/null 2>&1; then
    echo "start.sh: cargo not found on PATH; install the stable Rust toolchain" >&2
    exit 1
fi

for required in config/config.yml templates/index.html templates/channels.html; do
    if [[ ! -f "$required" ]]; then
        echo "start.sh: missing required file '$required'" >&2
        exit 1
    fi
done

export RUST_LOG="${RUST_LOG:-info}"

echo "starting soupcpy-agent on http://0.0.0.0:3001 (RUST_LOG=$RUST_LOG)"
echo "  controller: ${CONTROLLER_ENDPOINT:-<from config/config.yml>}"
echo "  agent udid: ${AGENT_UDID:-<from config/config.yml>}"

exec cargo run ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} -- ${BIN_ARGS[@]+"${BIN_ARGS[@]}"}
