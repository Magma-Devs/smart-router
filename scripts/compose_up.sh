#!/bin/bash

# =============================================================================
# compose_up.sh
#
# Bring up the minimal docker-compose stack for local smart-router dev:
#   <id>-router(s) + router-cache + Traefik (HTTP file provider on :80).
#
# Always rebuilds the static smart-router binary into build/smartrouter, then
# bakes it into a thin image via the root Dockerfile (BINARY_PATH build arg).
#
# Flags:
#   --reinstall          docker compose down -v before starting (drops volumes/networks)
#   --render-only        render compose files but don't run docker compose
# =============================================================================

set -e

REINSTALL_MODE=${REINSTALL_MODE:-false}
RENDER_ONLY=false

for arg in "$@"; do
    case $arg in
        --reinstall)         REINSTALL_MODE=true ;;
        --render-only)       RENDER_ONLY=true ;;
        --help|-h)
            sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="$REPO_ROOT/scripts"
cd "$REPO_ROOT"

source "$SCRIPT_DIR/utils/colors.sh"

VALUES_FILE="config/values.yml"
if [ ! -f "$VALUES_FILE" ]; then
    print_error "❌ Values file not found: $VALUES_FILE"
    exit 1
fi

print_section "Compose Stack: smart-router"
echo ""

if [ "$RENDER_ONLY" != true ]; then
    print_section "Building smart-router binary (static, stripped)"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build -trimpath -tags netgo -ldflags '-w -s' \
        -o build/smartrouter ./cmd/smartrouter
    print_success "✓ build/smartrouter"
fi

python3 "$SCRIPT_DIR/render_compose.py"

if [ "$RENDER_ONLY" = true ]; then
    print_success "✓ Render complete (--render-only)"
    exit 0
fi

COMPOSE_ARGS=(-f compose/docker-compose.yml)

if [ "$REINSTALL_MODE" = true ]; then
    print_section "Tearing down (REINSTALL)"
    docker compose "${COMPOSE_ARGS[@]}" down -v || true
fi

print_section "Bringing up smart-router stack"
docker compose "${COMPOSE_ARGS[@]}" up -d --build --remove-orphans
print_success "✓ Stack is up"
echo ""
echo "Endpoints:"
echo "  - Traefik admin:      http://localhost:8090"
echo ""
if [ -f compose/usage.txt ]; then
    cat compose/usage.txt
fi
echo "Logs:  docker compose -f compose/docker-compose.yml logs -f"
echo "Down:  ./scripts/compose_down.sh"
