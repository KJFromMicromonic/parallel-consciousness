#!/usr/bin/env bash
# Drive one live run of the two-service fixture with two real coding agents.
#
# This is an operator tool, not CI. Its pre-flight exists because two live runs
# failed for reasons outside the code entirely: one silently tested a stale pc
# binary for nine minutes, and one hung forever because a backgrounded `pi -p`
# inherited an open stdin. Both are checked below before anything starts.
#
# Pass --preflight-only to run every pre-flight check (root, harness smoke
# test, pc build and provenance) and then exit 0 without touching the
# fixture, starting any daemon, or launching any agent. That is a real thing
# an operator wants before committing to a run that can take several minutes
# per agent — and it is exactly the checking that would have prevented both
# failures above.
set -euo pipefail
# set -m: put each backgrounded job in its own process group, so the whole
# group can be signalled at once. Without this, killing a launch()'s $! only
# reaches the wrapping subshell — `( cd "$wt" && ... pi/claude ... ) &` is not
# exec-optimized away, so the harness runs as the subshell's child and is
# reparented, orphaned, and left running when the subshell dies. That is what
# survived a plain `kill` during this script's own verification.
set -m

PREFLIGHT_ONLY=0
if [ "${1:-}" = "--preflight-only" ]; then
  PREFLIGHT_ONLY=1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

RUN_DIR="${RUN_DIR:-$ROOT/.pc/live-$(date +%Y%m%d-%H%M%S)}"
BIN_DIR="$RUN_DIR/bin"
LOG_DIR="$RUN_DIR/logs"
WORK_DIR="$RUN_DIR/worktrees"
FIXTURE="$ROOT/fixtures/two-service"

mkdir -p "$BIN_DIR" "$LOG_DIR" "$WORK_DIR"

say() { printf '\n=== %s\n' "$*"; }
die() { printf '\nlive-run: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- pre-flight

say "pre-flight"

[ -f "$ROOT/go.mod" ] || die "not at the project root (no go.mod at $ROOT)"
[ -d "$FIXTURE" ] || die "fixture missing at $FIXTURE"

# Harness smoke test, before the (slower) build: an operator with no harness
# on PATH gets stopped by that fact directly, not by a "go build failed" that
# has nothing to do with their actual problem. A harness that cannot even
# report its version will not survive a nine-minute run, and finding that out
# now costs seconds.
have_pi=0
have_claude=0
if command -v pi >/dev/null && pi --version >/dev/null 2>&1; then
  have_pi=1
  printf 'pi: %s (%s)\n' "$(command -v pi)" "$(pi --version 2>&1 | head -1)"
fi
if command -v claude >/dev/null && claude --version >/dev/null 2>&1; then
  have_claude=1
  printf 'claude: %s (%s)\n' "$(command -v claude)" "$(claude --version 2>&1 | head -1)"
fi
if [ "$have_pi" -eq 0 ] && [ "$have_claude" -eq 0 ]; then
  die "no verified harness found: install pi or claude (see docs/recipes/)"
fi
if [ "$have_pi" -eq 0 ] || [ "$have_claude" -eq 0 ]; then
  printf '\nlive-run: only one harness available; running both agents on it.\n'
  printf 'The two-vendor configuration is what the agnostic claim rests on.\n'
fi

# Binary provenance. Built here, now, from this checkout — not found on PATH,
# where it may be any age. This is the check that a stale binary defeated.
say "building pc from $ROOT"
go build -o "$BIN_DIR/pc" ./cmd/pc || die "go build ./cmd/pc failed"
export PATH="$BIN_DIR:$PATH"
command -v pc >/dev/null || die "pc not on PATH after build"
resolved="$(command -v pc)"
[ "$resolved" = "$BIN_DIR/pc" ] || die "pc resolves to $resolved, not the binary just built at $BIN_DIR/pc"
printf 'pc: %s\n' "$resolved"

if [ "$PREFLIGHT_ONLY" -eq 1 ]; then
  say "pre-flight only: stopping before touching the fixture or starting anything"
  exit 0
fi

# ------------------------------------------------------------- fixture reset

say "resetting the fixture"

# Each agent gets its own worktree of the fixture repo. The fixture is its own
# git repository so a run never touches project source; initialise it once.
if [ ! -d "$FIXTURE/.git" ]; then
  git -C "$FIXTURE" init -q
  # Pin the branch to main before the first commit rather than relying on
  # init.defaultBranch, which is unset on plenty of machines (this one
  # included) and falls back to `master`. mergeAll in
  # internal/pcops/rungate.go hardcodes `git reset --hard -q main` in the
  # runner's worktree, so an unpinned branch name fails every round on an
  # unknown-revision git error that has nothing to do with the agents' work.
  # symbolic-ref works on every git version (unlike `init -b`, which needs
  # >= 2.28), and this script has no other git-version floor to lean on.
  git -C "$FIXTURE" symbolic-ref HEAD refs/heads/main
  git -C "$FIXTURE" add -A
  git -C "$FIXTURE" -c user.email=live-run@local -c user.name=live-run commit -qm "fixture baseline"
fi

# A worktree add checks out the last commit, silently, whatever it is. Local
# edits made to the fixture since the last run would otherwise be tested as
# stale content with no error and no message — the same class of surprise
# this script's provenance check exists to prevent for the pc binary, applied
# here to the fixture. Commit is the right response, not die: "reset the
# fixture" should mean what's on disk is what gets tested, but it must be
# announced.
if [ -n "$(git -C "$FIXTURE" status --porcelain)" ]; then
  printf 'live-run: fixture has uncommitted changes, committing as the new baseline:\n'
  git -C "$FIXTURE" status --porcelain
  git -C "$FIXTURE" add -A
  git -C "$FIXTURE" -c user.email=live-run@local -c user.name=live-run commit -qm "fixture baseline (auto, live-run)"
fi

for spec in "billing:agent/billing" "gateway:agent/gateway" "integrator:agent/integration"; do
  name="${spec%%:*}"; branch="${spec##*:}"
  path="$WORK_DIR/$name"
  git -C "$FIXTURE" worktree remove --force "$path" 2>/dev/null || true
  git -C "$FIXTURE" branch -D "$branch" 2>/dev/null || true
  git -C "$FIXTURE" worktree add -q -b "$branch" "$path" HEAD
  printf '%-11s %s (%s)\n' "$name" "$path" "$branch"
done

# --------------------------------------------------------------- the scenario

say "writing the scenario"

export PC_DB="$RUN_DIR/bus.db"
CONFIG="$RUN_DIR/pc.yaml"
( cd "$RUN_DIR" && pc init --force >/dev/null ) || die "pc init --force failed in $RUN_DIR"
sed -e "s|^repo: .*|repo: $FIXTURE|" -e "s|^db: .*|db: $PC_DB|" \
  "$RUN_DIR/.pc.yaml" > "$CONFIG" || die "writing $CONFIG from $RUN_DIR/.pc.yaml failed"
pc watch --config "$CONFIG" --no-follow >/dev/null || die "the scenario at $CONFIG does not load"
printf 'scenario: %s\n' "$CONFIG"

# ----------------------------------------------------------------- the daemons

say "starting the coordinator and the runner"

pc up --config "$CONFIG" >"$LOG_DIR/up.log" 2>&1 < /dev/null &
UP_PID=$!
pc run-gate --config "$CONFIG" --workdir "$WORK_DIR/integrator" >"$LOG_DIR/run-gate.log" 2>&1 < /dev/null &
GATE_PID=$!
pc watch --config "$CONFIG" --all --full >"$LOG_DIR/watch.log" 2>&1 < /dev/null &
WATCH_PID=$!

# Killed by process group (negative PID), not by PID: with set -m each of
# these is its own group leader, so -$pid reaches it and every child. This is
# what actually stops a launched agent — killing launch()'s bare $! only
# reaches the wrapping subshell in launch() and leaves the harness (pi/claude)
# orphaned, which is exactly what survived a plain kill during this script's
# own verification. Group-killing the three daemons this way is harmless:
# each is a single command, so its group contains only itself.
# ${AGENT_PIDS[@]+"${AGENT_PIDS[@]}"} rather than "${AGENT_PIDS[@]}" so this
# is safe under set -u if cleanup fires before any agent has launched (e.g.
# a die() during the daemon-liveness check below) — bash 3.2, which macOS
# ships, has no other way to iterate a possibly-unset array without erroring.
cleanup() {
  local pid
  for pid in "$UP_PID" "$GATE_PID" "$WATCH_PID" ${AGENT_PIDS[@]+"${AGENT_PIDS[@]}"}; do
    kill -- -"$pid" 2>/dev/null || true
  done
  for pid in "$UP_PID" "$GATE_PID" "$WATCH_PID" ${AGENT_PIDS[@]+"${AGENT_PIDS[@]}"}; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# StartCoordinator returns only after its subscription is live, but these are
# separate processes — give them a moment to reach that point before any agent
# can declare readiness into a log nobody is watching.
sleep 2
kill -0 "$UP_PID" 2>/dev/null || die "pc up exited immediately; see $LOG_DIR/up.log"
kill -0 "$GATE_PID" 2>/dev/null || die "pc run-gate exited immediately; see $LOG_DIR/run-gate.log"

# ------------------------------------------------------------------ the agents

say "launching the agents"

launch() {
  local name="$1" harness="$2" task="$3" wt="$WORK_DIR/$1"
  # < /dev/null on BOTH harnesses. `pi -p` merges piped stdin into the prompt,
  # so a backgrounded invocation with an inherited stdin blocks forever with no
  # output. This cost two nine-minute runs.
  case "$harness" in
    pi)
      ( cd "$wt" && PC_AGENT="$name" PC_TASK="$task" PC_DB="$PC_DB" \
        pi --skill "$ROOT/docs/agent-contract.md" -p "$task" < /dev/null ) \
        >"$LOG_DIR/$name.log" 2>&1 &
      ;;
    claude)
      cp "$ROOT/docs/agent-contract.md" "$wt/CLAUDE.md"
      ( cd "$wt" && PC_AGENT="$name" PC_TASK="$task" PC_DB="$PC_DB" \
        claude -p "$task" --permission-mode acceptEdits \
        --allowedTools Read Edit Write Bash < /dev/null ) \
        >"$LOG_DIR/$name.log" 2>&1 &
      ;;
  esac
  # Set explicitly rather than leaving the caller to read $!: a background
  # job started inside a function does set $! in the calling shell, but that
  # is subtle enough to be worth not depending on.
  LAST_PID=$!
  printf '%-11s %s (pid %d, log %s)\n' "$name" "$harness" "$LAST_PID" "$LOG_DIR/$name.log"
}

# Two vendors when both are available — that pairing is the configuration the
# harness-agnostic claim actually rests on.
if [ "$have_pi" -eq 1 ]; then billing_harness=pi; else billing_harness=claude; fi
if [ "$have_claude" -eq 1 ]; then gateway_harness=claude; else gateway_harness=pi; fi

launch billing "$billing_harness" \
  "Render the invoice currency correctly. You own billing/ only. Submit with: pc submit --gate currency --as billing"
AGENT_PIDS=("$LAST_PID")
launch gateway "$gateway_harness" \
  "Stamp the agreed currency on invoices you build. You own gateway/ only. Submit with: pc submit --gate currency --as gateway"
AGENT_PIDS+=("$LAST_PID")

# --------------------------------------------------------------------- report

say "waiting for the agents"

# A bare `wait` here trusted the daemons to still be there when the agents
# finished — checked once, two seconds after startup, and never again. If
# `pc up` or `pc run-gate` dies mid-run, an operator would otherwise watch a
# dead gate for the rest of the run: the same nine-minute waste this script
# exists to stop, just moved to a different daemon. Poll instead: as long as
# an agent is still running, check every few seconds that both daemons still
# are too, and stop waiting the moment one of them is not.
daemon_died=""
while :; do
  any_agent_alive=0
  for pid in ${AGENT_PIDS[@]+"${AGENT_PIDS[@]}"}; do
    kill -0 "$pid" 2>/dev/null && any_agent_alive=1
  done
  [ "$any_agent_alive" -eq 1 ] || break

  if ! kill -0 "$UP_PID" 2>/dev/null; then
    daemon_died="pc up exited mid-run; see $LOG_DIR/up.log"
    break
  fi
  if ! kill -0 "$GATE_PID" 2>/dev/null; then
    daemon_died="pc run-gate exited mid-run; see $LOG_DIR/run-gate.log"
    break
  fi
  sleep 3
done

if [ -n "$daemon_died" ]; then
  printf '\nlive-run: %s\n' "$daemon_died"
  printf 'live-run: the gate is gone; not waiting on the agents further.\n'
else
  for pid in ${AGENT_PIDS[@]+"${AGENT_PIDS[@]}"}; do wait "$pid" 2>/dev/null || true; done
fi

say "report"
printf 'run directory: %s\n\n' "$RUN_DIR"
printf 'gate activity:\n'
grep -E "ready|ack|nack|request|inform|block" "$LOG_DIR/watch.log" | tail -40 || true
printf '\nverdicts seen by the coordinator:\n'
grep -E "PASSED|FAILED|STALLED" "$LOG_DIR/up.log" || printf '  (none)\n'
printf '\nlogs: %s\n' "$LOG_DIR"

if grep -q "PASSED" "$LOG_DIR/up.log" 2>/dev/null; then
  printf '\nlive-run: the gate PASSED.\n'
  exit 0
fi
printf '\nlive-run: no passing verdict. Read %s and %s.\n' "$LOG_DIR/watch.log" "$LOG_DIR/up.log"
exit 1
