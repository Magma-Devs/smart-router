#!/bin/bash

# Tear down the local docker-compose stack.
# Default keeps named volumes (none in this minimal stack, but harmless).
# Pass --volumes / -v or --reinstall to also drop volumes.

set -e

REINSTALL_MODE=${REINSTALL_MODE:-false}
DROP_VOLUMES=false

for arg in "$@"; do
    case $arg in
        --volumes|-v) DROP_VOLUMES=true ;;
        --reinstall)  REINSTALL_MODE=true ;;
        --help|-h)
            echo "Usage: $0 [--volumes|-v] [--reinstall]"
            exit 0
            ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DOWN_ARGS=()
if [ "$DROP_VOLUMES" = true ] || [ "$REINSTALL_MODE" = true ]; then
    DOWN_ARGS+=(-v)
fi

docker compose -f compose/docker-compose.yml down --remove-orphans "${DOWN_ARGS[@]}"
echo "✓ Stack torn down${DROP_VOLUMES:+ (volumes dropped)}"

# Also stop the buildkit container `docker compose --build` spawns when a
# docker-container buildx builder is active. It would otherwise sit around
# indefinitely after every compose_down — small but surprising. Cold-starting
# it next build is ~5s; cache state on the disk volume persists.
# (Probing via `docker ps` rather than `docker buildx inspect --bootstrap`,
# which would restart a stopped builder as a side effect.)
BUILDKIT_CONTAINERS=$(docker ps -q --filter "name=buildx_buildkit_" 2>/dev/null)
if [ -n "$BUILDKIT_CONTAINERS" ]; then
    docker stop $BUILDKIT_CONTAINERS >/dev/null 2>&1 || true
    echo "✓ Buildkit container(s) stopped"
fi
