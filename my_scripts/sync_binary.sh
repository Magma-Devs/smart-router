#!/bin/bash
# Build smart-router binary on Mac, scp to server, rolling-restart all *-router deployments.
# Run from anywhere: bash /Users/victoria/smart-router/my_scripts/sync_binary.sh

set -e

REMOTE="root@64.176.170.39"
SSH_OPTS=(-o RemoteCommand=none -o RequestTTY=no)
LOCAL_REPO="/Users/victoria/smart-router"

cd "${LOCAL_REPO}"

echo "==> 1/4 pre-flight: source state at ${LOCAL_REPO}"
BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT=$(git rev-parse --short HEAD)
SUBJECT=$(git log -1 --pretty=format:'%s')
if git diff-index --quiet HEAD --; then
  DIRTY=""
else
  DIRTY="yes"
fi
UPSTREAM=$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)

echo "  branch:   ${BRANCH}"
echo "  commit:   ${COMMIT} — ${SUBJECT}"
[ -n "${DIRTY}" ] && echo "  ⚠️  uncommitted changes present in working tree"
if [ -n "${UPSTREAM}" ]; then
  git fetch --quiet origin "${UPSTREAM#origin/}" 2>/dev/null || true
  BEHIND=$(git rev-list --count "HEAD..${UPSTREAM}" 2>/dev/null || echo "0")
  AHEAD=$(git rev-list --count "${UPSTREAM}..HEAD" 2>/dev/null || echo "0")
  echo "  vs ${UPSTREAM}: ${AHEAD} ahead, ${BEHIND} behind"
  [ "${BEHIND}" != "0" ] && echo "  ⚠️  branch is behind ${UPSTREAM} — consider 'git pull' first"
else
  BEHIND="0"
  echo "  vs upstream: (no upstream configured)"
fi

if [ -n "${DIRTY}" ] || [ "${BEHIND}" != "0" ]; then
  echo ""
  read -p "Proceed anyway? (y/N) " -n 1 -r
  echo
  if [[ ! ${REPLY} =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
  fi
fi
echo ""

echo "==> 2/4 build smart-router-binary (linux/amd64, fully static, stripped)"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -mod=readonly -trimpath -tags netgo -ldflags '-w -s' -o build/smart-router-binary ./cmd/smartrouter

echo "==> 3/4 scp ./build/smart-router-binary -> ${REMOTE}:/root/smart-router-binary (via .new + atomic mv)"
# Stage to .new then atomic rename to avoid ETXTBSY: running pods hold the old
# binary's inode open for execution, so overwriting /root/smart-router-binary
# in place is refused by the kernel. mv on the same filesystem swaps only the
# directory entry; the running inode is untouched and pods pick up the new
# file on their next restart (triggered below).
scp "${SSH_OPTS[@]}" "${LOCAL_REPO}/build/smart-router-binary" "${REMOTE}:/root/smart-router-binary.new"
ssh "${SSH_OPTS[@]}" "${REMOTE}" "mv /root/smart-router-binary.new /root/smart-router-binary"

echo "==> 4/4 rolling-restart every *-router deployment in lava-infra"
ssh "${SSH_OPTS[@]}" "${REMOTE}" "kubectl get deploy -n lava-infra -o name | grep -- '-router' | xargs -I {} kubectl rollout restart {} -n lava-infra"

echo ""
echo "Done. Watch pods come back up with:"
echo "  ssh ${REMOTE} 'kubectl get pods -n lava-infra'"
