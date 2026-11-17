#!/usr/bin/env bash
# End-to-end check of a real 3-node cluster on 127.0.0.1:5161-5163.
#
#   scripts/smoke.sh                 start three local processes (needs a Release build)
#   MODE=compose scripts/smoke.sh    use docker compose (docker-compose.yml)
#
# Writes through a follower (307 to the leader), reads, compare-and-sets, deletes, stops the leader,
# waits for a new one, checks the data survived, restarts the old leader and reads through it.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${MODE:-local}"
PORTS=(0 5161 5162 5163)
PEERS="1=http://127.0.0.1:5161,2=http://127.0.0.1:5162,3=http://127.0.0.1:5163"
DATA="$ROOT/.cluster/smoke"
DLL="$ROOT/src/Concord.Server/bin/Release/net8.0/Concord.Server.dll"
declare -a PIDS=()

url() { echo "http://127.0.0.1:${PORTS[$1]}"; }
log() { printf '[smoke] %s\n' "$*"; }
fail() { log "FAILED: $*"; dump_logs; exit 1; }

dump_logs() {
  if [[ "$MODE" == compose ]]; then
    docker compose -f "$ROOT/docker-compose.yml" logs --tail 40 || true
  else
    for i in 1 2 3; do [[ -f "$DATA/n$i.log" ]] && { echo "--- n$i"; tail -n 20 "$DATA/n$i.log"; }; done
  fi
}

start_node() {
  local i=$1
  if [[ "$MODE" == compose ]]; then
    docker compose -f "$ROOT/docker-compose.yml" start "n$i" >/dev/null
  else
    dotnet "$DLL" --urls "$(url "$i")" --node-id "$i" --peers "$PEERS" --data-dir "$DATA/n$i" >>"$DATA/n$i.log" 2>&1 &
    PIDS[$i]=$!
  fi
}

stop_node() {
  local i=$1
  if [[ "$MODE" == compose ]]; then
    docker compose -f "$ROOT/docker-compose.yml" stop "n$i" >/dev/null
  else
    kill "${PIDS[$i]}" 2>/dev/null || true
    wait "${PIDS[$i]}" 2>/dev/null || true
  fi
}

cleanup() {
  if [[ "$MODE" == compose ]]; then
    docker compose -f "$ROOT/docker-compose.yml" down -v >/dev/null 2>&1 || true
  else
    for i in 1 2 3; do [[ -n "${PIDS[$i]:-}" ]] && stop_node "$i"; done
  fi
}
trap cleanup EXIT

field() { sed -n "s/.*\"$1\":\"\{0,1\}\([^,\"}]*\)\"\{0,1\}[,}].*/\1/p"; }

status() { curl -s --max-time 2 "$(url "$1")/cluster/status" || true; }

# Prints the id of a node that reports itself leader with a committed entry, among the given nodes.
wait_for_leader() {
  local deadline=$((SECONDS + 30))
  while ((SECONDS < deadline)); do
    for i in "$@"; do
      local s; s="$(status "$i")"
      if [[ "$(field role <<<"$s")" == Leader && "$(field commitIndex <<<"$s")" -gt 0 ]]; then
        echo "$i"; return 0
      fi
    done
    sleep 0.2
  done
  return 1
}

expect() { # expect <description> <expected> <actual>
  [[ "$2" == "$3" ]] || fail "$1: expected '$2', got '$3'"
  log "ok: $1"
}

if [[ "$MODE" == compose ]]; then
  docker compose -f "$ROOT/docker-compose.yml" up -d --build
else
  [[ -f "$DLL" ]] || fail "build first: dotnet build -c Release"
  rm -rf "$DATA" && mkdir -p "$DATA"
  for i in 1 2 3; do start_node "$i"; done
fi

leader="$(wait_for_leader 1 2 3)" || fail "no leader within 30 s"
follower=$((leader % 3 + 1))
log "leader is n$leader"

code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT --data 'hello' "$(url "$follower")/kv/greeting")"
expect "follower redirects writes" 307 "$code"

curl -sf -L -X PUT --data 'hello' "$(url "$follower")/kv/greeting" >/dev/null || fail "PUT via redirect"
expect "read back" hello "$(curl -sf -L "$(url "$follower")/kv/greeting" | field value)"

curl -sf -L -X POST -H 'Content-Type: application/json' --data '{"expected":null,"value":"n1"}' "$(url "$leader")/kv/lock/cas" >/dev/null || fail "first CAS"
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data '{"expected":null,"value":"n2"}' "$(url "$leader")/kv/lock/cas")"
expect "second CAS conflicts" 409 "$code"

curl -sf -X PUT --data 'temporary' "$(url "$leader")/kv/scratch" >/dev/null
expect "delete reports existing key" true "$(curl -sf -X DELETE "$(url "$leader")/kv/scratch" | field deleted)"
expect "deleted key is gone" 404 "$(curl -s -o /dev/null -w '%{http_code}' "$(url "$leader")/kv/scratch")"

log "stopping leader n$leader"
stop_node "$leader"
others=(); for i in 1 2 3; do [[ $i -ne $leader ]] && others+=("$i"); done
next="$(wait_for_leader "${others[@]}")" || fail "no new leader after stopping n$leader"
log "new leader is n$next"
expect "data survives leader loss" hello "$(curl -sf "$(url "$next")/kv/greeting" | field value)"
curl -sf -X PUT --data 'after-failover' "$(url "$next")/kv/greeting" >/dev/null || fail "write after failover"

log "restarting n$leader"
start_node "$leader"
deadline=$((SECONDS + 30))
until [[ "$(status "$leader" | field leaderId)" == "$next" ]]; do
  ((SECONDS < deadline)) || fail "n$leader did not rejoin"
  sleep 0.2
done
expect "old leader rejoins as follower and serves reads via redirect" after-failover "$(curl -sf -L "$(url "$leader")/kv/greeting" | field value)"

log "all checks passed"
