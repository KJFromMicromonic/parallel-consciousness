# Sticky Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make readiness a standing claim about a version rather than an event the round consumes, so a participant whose half is already correct cannot starve the gate by finishing and exiting.

**Architecture:** Verdicts become truthful first (the runner reports the SHAs it actually merged, and that reaches the coordinator over the wire), then `resolve` stops clearing `gs.ready` and updates it from those truthful versions instead. Two follow-on changes close the gaps this opens: a distinct `Submit` outcome when the gate tested a different version than was declared, and a `running` flag on the acknowledgement so the now-common empty `outstanding` list stops rendering as silence.

**Tech Stack:** Go 1.22 (stdlib plus `modernc.org/sqlite`, `gopkg.in/yaml.v3`, `github.com/google/uuid`), git worktrees, SQLite.

**Spec:** `docs/superpowers/specs/2026-09-13-sticky-readiness-design.md` — read it first. It carries the live-run evidence, the rejected alternatives, and the reasoning for each decision below.

## Global Constraints

- Go floor is `go 1.22`. Do not raise it.
- No new module dependencies. No `go get`. `modernc.org/sqlite` stays at `v1.33.1`.
- `bus.Bus` stays exactly two methods, `Publish` and `Subscribe`.
- `pkg/` is public API and may not import `internal/` at all — `internal/arch/arch_test.go` enforces it with a compiler check. `pkg/runtime` stays stdlib-only.
- `cmd/pc` stays a thin skin over `internal/pcops`.
- `internal/pcops/courier.go` is not touched by this plan, and no courier handler may be registered for `IntentAck` or `IntentNack`. The courier forwards into live coding-agent sessions; an earlier design that routed a coordinator message that way produced an unbounded feedback loop of 2,589 goroutines in 8 seconds.
- No `time.Sleep` in tests. A bounded `time.After` inside a `select`, used as a failure deadline, is correct and expected.
- `gofmt -l pkg internal cmd fixtures` must report only `cmd/sqlitedemo/main.go`, a documented pre-existing violation.
- Every existing test must keep passing unmodified unless a task explicitly says otherwise. This repo has found nine tests that looked like they tested something and did not; a test you had to change to make green deserves suspicion before it deserves an edit.

---

## File Structure

**Modified:**

| Path | Change |
|---|---|
| `internal/pcops/rungate.go` | `mergeAll` returns the SHAs it merged; `StartRunner` maps branch → participant and reports them. |
| `internal/pcops/rungate_test.go` | Tests for the merged-SHA reporting. |
| `pkg/gate/gate.go` | `ServeRunner`'s reply carries versions; `onVerdictMsg` decodes them; `resolve` stops clearing `gs.ready` and merges truthful versions into it; the Ack gains `running`. |
| `pkg/gate/gate_test.go` | The deadlock test, the no-redundant-round test, the F2 replay, back-compat, and the `running` flag. |
| `internal/pcops/submit.go` | New sentinel and the select arm that returns it. |
| `internal/pcops/submit_test.go` | The fast-return test for the new sentinel. |
| `internal/pcops/watch.go` | Render the Ack's `running` flag. |
| `internal/pcops/watch_test.go` | Pin that rendering. |

**Task order matters.** Tasks 1–4 make verdicts truthful; Task 5 is the switch that makes readiness standing and is the one that fixes the reported bug. Doing 5 first would work — `resolve` would merge the backfilled values into `gs.ready`, a no-op — but Task 6's sentinel only means anything once verdicts are truthful, so the order below keeps each task's tests honest.

---

## Two hazards this codebase has already been bitten by

**1. Tests that look like they test something and don't.** Nine found so far. The pattern to watch for in this plan specifically: a deadlock test in which the "exiting" participant actually resubmits proves nothing, and a fast-return test with a generous deadline passes against the very hang it exists to prevent. For every test here, ask what single-line production change would make it fail.

**2. The subshell-versus-child confusion.** Not relevant to this plan's code, but it is why `scripts/live-run.sh` exists in its current shape — do not run that script without `--preflight-only`; it launches real, billable coding agents.

---

## Task 1: `mergeAll` reports the SHAs it merged

**Files:**
- Modify: `internal/pcops/rungate.go:91` (`mergeAll`)
- Test: `internal/pcops/rungate_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `mergeAll(ctx, workdir, branches) (map[string]string, string, error)` — merged SHAs keyed by **branch name**, then the existing detail string and error. Returns a nil map on any failure. Task 3 consumes this.

- [ ] **Step 1: Write the failing test**

Add to `internal/pcops/rungate_test.go`:

```go
// The verdict has to be able to say what was actually tested, which means the
// runner must report the commit it merged rather than echoing back the version
// a participant declared. Those two diverge the moment an agent commits again
// after submitting, and a verdict naming the declared value is then a lie about
// what ran.
func TestMergeAllReportsTheShasItMerged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := t.TempDir()
	git(t, repo, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\n"), 0o644)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "init")

	want := map[string]string{}
	for _, b := range []string{"a", "b"} {
		git(t, repo, "checkout", "-q", "-b", b, "main")
		os.WriteFile(filepath.Join(repo, b+".txt"), []byte(b), 0o644)
		git(t, repo, "add", ".")
		git(t, repo, "commit", "-q", "-m", b)
		out, err := exec.Command("git", "-C", repo, "rev-parse", b).Output()
		if err != nil {
			t.Fatal(err)
		}
		want[b] = strings.TrimSpace(string(out))
	}
	git(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	git(t, repo, "worktree", "add", "-B", "agent/integration", work)

	merged, detail, err := mergeAll(ctx, work, []string{"a", "b"})
	if err != nil {
		t.Fatalf("mergeAll: %v (%s)", err, detail)
	}
	if len(merged) != 2 {
		t.Fatalf("merged = %v, want an entry per branch", merged)
	}
	for br, sha := range want {
		if merged[br] != sha {
			t.Errorf("merged[%q] = %q, want the branch tip %q", br, merged[br], sha)
		}
	}
}

// A failed merge is aborted, so nothing coherent was tested and there is no
// honest SHA to name. Reporting a partial set would let a verdict claim it
// tested branches whose merge was rolled back.
func TestMergeAllReportsNothingWhenAMergeFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := t.TempDir()
	git(t, repo, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(repo, "f.txt"), []byte("base\n"), 0o644)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "init")

	// Two branches that edit the same line: the second merge conflicts.
	for _, b := range []string{"x", "y"} {
		git(t, repo, "checkout", "-q", "-b", b, "main")
		os.WriteFile(filepath.Join(repo, "f.txt"), []byte(b+"\n"), 0o644)
		git(t, repo, "add", ".")
		git(t, repo, "commit", "-q", "-m", b)
	}
	git(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	git(t, repo, "worktree", "add", "-B", "agent/integration", work)

	merged, detail, err := mergeAll(ctx, work, []string{"x", "y"})
	if err == nil {
		t.Fatalf("mergeAll succeeded on conflicting branches; detail=%q merged=%v", detail, merged)
	}
	if merged != nil {
		t.Errorf("merged = %v, want nil: the merge was aborted, so nothing was tested", merged)
	}
	if !strings.Contains(detail, "conflict") {
		t.Errorf("detail = %q, want it to name the conflict", detail)
	}
}
```

This test file needs `os/exec` and `strings` in its imports if they are not already there — check before adding.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/pcops/ -run TestMergeAllReports -v`
Expected: FAIL to compile — `mergeAll` returns two values, not three: `assignment mismatch: 3 variables but mergeAll returns 2 values`.

- [ ] **Step 3: Change `mergeAll`**

Replace `internal/pcops/rungate.go`'s `mergeAll` with:

```go
// mergeAll resets the runner's worktree to main and merges each participant's
// branch into it, returning the commit it merged per branch.
//
// The returned map is what makes a verdict truthful. The coordinator otherwise
// backfills a verdict's Versions from the readiness it recorded — the versions
// participants DECLARED — and those diverge from what was tested the moment an
// agent commits again after submitting. Reporting the tip that actually went in
// is the only way the verdict can describe the run rather than the intent.
//
// On any failure it returns a nil map: the merge is aborted, so nothing
// coherent was tested and there is no honest SHA to report.
func mergeAll(ctx context.Context, workdir string, branches []string) (map[string]string, string, error) {
	// Deliberately no `checkout main`: main is checked out in the primary
	// worktree, and git refuses to check out a branch twice. Resetting the
	// runner's own branch to main achieves the same clean baseline.
	for _, args := range [][]string{
		{"reset", "--hard", "-q", "main"},
		{"clean", "-qfd"},
	} {
		if out, err := gitIn(ctx, workdir, args...); err != nil {
			return nil, trim(out), err
		}
	}
	merged := make(map[string]string, len(branches))
	for _, br := range branches {
		out, err := gitIn(ctx, workdir, "merge", "--no-edit", "-q", br)
		if err == nil {
			sha, shaErr := gitIn(ctx, workdir, "rev-parse", br)
			if shaErr != nil {
				return nil, fmt.Sprintf("merged %s but could not resolve its tip: %v: %s", br, shaErr, trim(sha)), shaErr
			}
			merged[br] = trim(sha)
			continue
		}
		// Classify before aborting: `merge --abort` clears the unmerged index
		// this reads. Calling every non-zero exit a conflict sent both owners
		// hunting a conflict that did not exist whenever the real cause was a
		// missing branch or transient ref/index.lock contention — the detail an
		// agent is steered with has to name what actually happened.
		conflict := isMergeConflict(err, out) || hasUnmergedPaths(ctx, workdir)
		_, _ = gitIn(ctx, workdir, "merge", "--abort")
		if conflict {
			return nil, fmt.Sprintf("merge conflict on %s: %s", br, trim(out)), err
		}
		return nil, fmt.Sprintf("merge failed on %s: %v: %s", br, err, trim(out)), err
	}
	return merged, "", nil
}
```

- [ ] **Step 4: Fix the one existing call site**

In `StartRunner`'s `ServeRunner` callback (`rungate.go:56`), the call currently reads `if detail, err := mergeAll(ctx, workdir, branches); err != nil`. Change it to discard the new first return for now — Task 3 will use it:

```go
		if _, detail, err := mergeAll(ctx, workdir, branches); err != nil {
```

- [ ] **Step 5: Run the tests**

Run: `go test ./internal/pcops/ -run 'TestMergeAll|TestRunGate' -v`
Expected: PASS, including the existing `TestRunGateMergesBranchesBeforeTesting`.

Then `go test ./... -count=1`.

- [ ] **Step 6: Commit**

```bash
git add internal/pcops/rungate.go internal/pcops/rungate_test.go
git commit -m "feat(pcops): mergeAll reports the commits it merged

A verdict cannot describe what ran while the only versions available are the
ones participants declared. Those diverge from what was tested whenever an
agent commits again after submitting. Returns nil on failure: the merge is
aborted, so there is no honest SHA to name."
```

---

## Task 2: `ServeRunner`'s reply carries the versions

**Files:**
- Modify: `pkg/gate/gate.go:54` (`ServeRunner`)
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: nothing from Task 1 — this task is in `pkg/gate` and independent of it.
- Produces: the runner's reply body gains `"versions"` when the callback's Verdict has a non-nil `Versions`, and omits it otherwise. Task 4 consumes that field.

- [ ] **Step 1: Write the failing test**

Add to `pkg/gate/gate_test.go`:

```go
// The runner is the only participant that knows what was actually merged, so
// its reply is the only channel that can carry that back. Today ServeRunner
// builds a reply body of {gate, detail} and silently drops whatever Versions
// the callback set — internal/pcops/rungate.go has been setting that field on
// every return path and having it discarded.
func TestServeRunnerReplyCarriesVersions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	r, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(r, func(_ context.Context, gateID string, _ map[string]string) gate.Verdict {
		return gate.Verdict{
			GateID:   gateID,
			Passed:   true,
			Versions: map[string]string{"billing": "sha-b", "gateway": "sha-g"},
		}
	})
	go r.Run(ctx)

	asker, err := agent.New(ctx, b, "asker", nil)
	if err != nil {
		t.Fatal(err)
	}
	replies := make(chan protocol.Message, 4)
	asker.On(protocol.IntentDone, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		replies <- m
		return nil
	})
	go asker.Run(ctx)

	req := protocol.New(protocol.Address{Agent: "asker"}, protocol.Address{Agent: "runner"},
		protocol.IntentRequest, map[string]any{"gate": "g", "versions": map[string]string{"billing": "declared"}})
	if err := b.Publish(ctx, req); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-replies:
		got := protocol.Versions(m.Body["versions"])
		if got["billing"] != "sha-b" || got["gateway"] != "sha-g" {
			t.Fatalf("reply versions = %v, want the runner's reported shas", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no reply from the runner")
	}
}

// A runner that reports nothing — an older build, or a third-party
// implementation of this contract — must keep working. The coordinator's
// backfill is the compatibility path, and it only fires when the field is
// absent, so ServeRunner must not invent an empty map.
func TestServeRunnerOmitsVersionsWhenTheCallbackReportsNone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	r, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(r, func(_ context.Context, gateID string, _ map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true} // Versions left nil
	})
	go r.Run(ctx)

	asker, err := agent.New(ctx, b, "asker", nil)
	if err != nil {
		t.Fatal(err)
	}
	replies := make(chan protocol.Message, 4)
	asker.On(protocol.IntentDone, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		replies <- m
		return nil
	})
	go asker.Run(ctx)

	req := protocol.New(protocol.Address{Agent: "asker"}, protocol.Address{Agent: "runner"},
		protocol.IntentRequest, map[string]any{"gate": "g"})
	if err := b.Publish(ctx, req); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-replies:
		if _, present := m.Body["versions"]; present {
			t.Fatalf("reply carries a versions key for a callback that reported none: %+v", m.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no reply from the runner")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./pkg/gate/ -run TestServeRunner -v`
Expected: `TestServeRunnerReplyCarriesVersions` FAILS with `reply versions = map[], want the runner's reported shas`. `TestServeRunnerOmitsVersionsWhenTheCallbackReportsNone` PASSES already — it is the guard that the fix stays additive, so confirm it passes both before and after.

- [ ] **Step 3: Change `ServeRunner`**

In `pkg/gate/gate.go`, replace the reply construction inside `ServeRunner`:

```go
		reply := m.Reply(protocol.Address{Agent: ag.Name}, intent, map[string]any{
			"gate":   gateID,
			"detail": v.Detail,
		})
		// Only set when the runner actually reported. The coordinator backfills
		// a verdict's versions from the readiness it recorded whenever this key
		// is absent, and that backfill is the compatibility path for a runner
		// that does not report — an older build, or a third-party
		// implementation of this contract. Inventing an empty map here would
		// suppress the backfill and leave such a verdict with no versions at
		// all.
		if len(v.Versions) > 0 {
			reply.Body["versions"] = v.Versions
		}
		return &reply
```

- [ ] **Step 4: Run the tests**

Run: `go test ./pkg/gate/ -v -count=1`
Expected: PASS, both new tests and every existing one.

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): the runner's reply carries the versions it reports

ServeRunner built a reply of {gate, detail} and dropped whatever Versions the
callback set, which internal/pcops/rungate.go has been setting on all three of
its return paths. Set only when non-empty, so the coordinator's backfill stays
the compatibility path for a runner that reports nothing."
```

---

## Task 3: `StartRunner` reports merged SHAs per participant

**Files:**
- Modify: `internal/pcops/rungate.go:30` (`StartRunner`)
- Test: `internal/pcops/rungate_test.go`

**Interfaces:**
- Consumes: `mergeAll(ctx, workdir, branches) (map[string]string, string, error)` from Task 1; `ServeRunner`'s reply field from Task 2.
- Produces: the runner's Verdict now carries `Versions` keyed by **participant name**, not branch.

- [ ] **Step 1: Write the failing test**

Add to `internal/pcops/rungate_test.go`:

```go
// mergeAll keys its report by branch; the coordinator keys readiness by
// participant. The runner is the only place holding both halves of that
// mapping, and it must not lean on the ordering of the branch slice it was
// handed — that ordering is an implementation detail of cmd/pc, and making it
// load-bearing across a package boundary would break silently.
func TestRunGateVerdictNamesParticipantsNotBranches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	repo := t.TempDir()
	git(t, repo, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\n"), 0o644)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "init")
	git(t, repo, "checkout", "-q", "-b", "agent/billing", "main")
	os.WriteFile(filepath.Join(repo, "a.txt"), []byte("a"), 0o644)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "a")
	out, err := exec.Command("git", "-C", repo, "rev-parse", "agent/billing").Output()
	if err != nil {
		t.Fatal(err)
	}
	wantSHA := strings.TrimSpace(string(out))
	git(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	git(t, repo, "worktree", "add", "-B", "agent/integration", work)

	cfg := pcops.Config{
		DB:     filepath.Join(t.TempDir(), "bus.db"),
		GateID: "g",
		Gate:   pcops.GateDef{Required: []string{"billing"}, Runner: "integrator", Run: "test -f a.txt"},
		Agents: []pcops.AgentDef{{Name: "billing", Branch: "agent/billing"}},
	}

	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()
	rstop, err := pcops.StartRunner(ctx, cfg, work, []string{"agent/billing"})
	if err != nil {
		t.Fatal(err)
	}
	defer rstop()

	cfg.SubmitTimeout = 30 * time.Second
	// Deliberately declares a version that is NOT the branch tip. The verdict
	// must name what was merged, not what was claimed.
	v, err := pcops.Submit(ctx, cfg, "g", "billing", wantSHA)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if v.Versions["billing"] != wantSHA {
		t.Fatalf("verdict versions = %v, want it keyed by participant name with the merged sha %q", v.Versions, wantSHA)
	}
	if _, wrong := v.Versions["agent/billing"]; wrong {
		t.Errorf("verdict versions are keyed by BRANCH: %v", v.Versions)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/pcops/ -run TestRunGateVerdictNamesParticipants -v`
Expected: FAIL — the verdict's versions come from the coordinator's backfill, so `v.Versions["billing"]` holds the declared value. Because the test declares the branch tip as its version, this specific assertion may pass by coincidence; the `agent/billing` key assertion is what fails first if the mapping is wrong. Read the failure and confirm which assertion fired before implementing.

- [ ] **Step 3: Implement the branch → participant mapping**

In `StartRunner`, before the `ServeRunner` call, build the mapping from `cfg` rather than from the `branches` slice:

```go
	// mergeAll reports per branch; the coordinator keys readiness by
	// participant. Build the translation from cfg directly rather than zipping
	// it against the branches slice: that slice is produced by cmd/pc's
	// branchesFromConfig, whose ordering is an implementation detail of another
	// package, and a mismatch would mis-attribute every version silently.
	participantOf := make(map[string]string, len(cfg.Agents))
	for _, a := range cfg.Agents {
		participantOf[a.Branch] = a.Name
	}
```

Then rewrite the callback:

```go
	gate.ServeRunner(a, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		merged, detail, err := mergeAll(ctx, workdir, branches)
		if err != nil {
			// No versions: the merge was aborted, so nothing was tested and the
			// coordinator's backfill should describe the round instead.
			return gate.Verdict{GateID: gateID, Passed: false, Detail: detail}
		}
		tested := make(map[string]string, len(merged))
		for br, sha := range merged {
			if name, ok := participantOf[br]; ok {
				tested[name] = sha
			}
		}
		out, err := runShell(ctx, workdir, cfg.Gate.Run, runnerTimeout)
		if err != nil {
			return gate.Verdict{GateID: gateID, Passed: false, Detail: trim(out), Versions: tested}
		}
		return gate.Verdict{GateID: gateID, Passed: true, Versions: tested}
	})
```

Note the `versions` parameter is now unused in the body. Rename it to `_` to keep `go vet` and readers happy:

```go
	gate.ServeRunner(a, func(ctx context.Context, gateID string, _ map[string]string) gate.Verdict {
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/pcops/ -run 'TestRunGate|TestMergeAll' -v` then `go test ./... -count=1`.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/rungate.go internal/pcops/rungate_test.go
git commit -m "feat(pcops): the runner reports merged shas keyed by participant

mergeAll reports per branch and the coordinator keys readiness by participant;
the runner is the only place holding both halves. Built from cfg.Agents rather
than zipped against the branches slice, whose ordering belongs to cmd/pc."
```

---

## Task 4: The coordinator prefers the runner's versions

**Files:**
- Modify: `pkg/gate/gate.go:368` (`onVerdictMsg`)
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: the reply's `"versions"` field from Task 2.
- Produces: a Verdict reaching `resolve` with `Versions` already set when the runner reported. `resolve`'s existing `if v.Versions == nil` backfill at `gate.go:393` is untouched and becomes the compatibility path.

- [ ] **Step 1: Write the failing test**

Add to `pkg/gate/gate_test.go`:

```go
// The whole point of the runner reporting is that its report wins. A verdict
// must describe what the runner merged, not what the coordinator recorded when
// readiness was declared — those diverge whenever a branch moves after its
// owner submits.
func TestVerdictPrefersTheRunnersReportedVersions(t *testing.T) {
	h := setupGate(t, checkoutSpec(), func(gateID string, _ map[string]string) gate.Verdict {
		return gate.Verdict{
			GateID:   gateID,
			Passed:   true,
			Versions: map[string]string{"billing": "merged-b", "gateway": "merged-g"},
		}
	})
	defer h.cancel()

	h.ready(t, "billing", "declared-b")
	h.ready(t, "gateway", "declared-g")

	v := recvVerdict(t, h.verdict)
	if v.Versions["billing"] != "merged-b" || v.Versions["gateway"] != "merged-g" {
		t.Fatalf("versions = %v, want the runner's merged shas, not the declared ones", v.Versions)
	}
}

// And the inverse: a runner that reports nothing must still produce a verdict
// describing the round, via the coordinator's backfill. This is the
// compatibility path for an older or third-party runner.
func TestVerdictFallsBackToRecordedReadinessWhenTheRunnerReportsNone(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner) // passRunner leaves Versions nil
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")

	v := recvVerdict(t, h.verdict)
	if v.Versions["billing"] != "b1" || v.Versions["gateway"] != "g1" {
		t.Fatalf("versions = %v, want the recorded readiness as a fallback", v.Versions)
	}
}
```

- [ ] **Step 2: Run the tests to verify the first fails**

Run: `go test ./pkg/gate/ -run 'TestVerdictPrefers|TestVerdictFallsBack' -v`
Expected: `TestVerdictPrefersTheRunnersReportedVersions` FAILS with `versions = map[billing:declared-b gateway:declared-g]`. `TestVerdictFallsBackToRecordedReadinessWhenTheRunnerReportsNone` PASSES already and must keep passing.

- [ ] **Step 3: Decode the versions in `onVerdictMsg`**

In `pkg/gate/gate.go`, change `onVerdictMsg`:

```go
func (c *Coordinator) onVerdictMsg(ctx context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
	gateID, _ := m.Body["gate"].(string)
	gs := c.gate(gateID)
	if gs == nil {
		return nil
	}
	detail, _ := m.Body["detail"].(string)
	// The runner reports what it actually merged. When it does, that is the
	// truth about the round and it wins; resolve's backfill from recorded
	// readiness then fires only for a runner that reported nothing, which is
	// the compatibility path rather than the normal one.
	v := Verdict{
		GateID:   gateID,
		Passed:   m.Intent == protocol.IntentDone,
		Detail:   detail,
		Versions: protocol.Versions(m.Body["versions"]),
	}
	c.resolve(ctx, gs, v, false, false)
	return nil
}
```

`protocol.Versions` returns nil for an absent key, so the backfill condition at `gate.go:393` still triggers correctly.

- [ ] **Step 4: Run the tests**

Run: `go test ./pkg/gate/ -v -count=1` then `go test ./... -count=1`.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): a verdict prefers the versions the runner reported

resolve's backfill from recorded readiness becomes the compatibility path for a
runner that reports nothing, rather than the only path. protocol.Versions
returns nil for an absent key, so that condition still triggers correctly."
```

---

## Task 5: Standing readiness

This is the task that fixes the reported bug. Read the spec's Motivation section before starting; the bus log there is what this implements.

**Files:**
- Modify: `pkg/gate/gate.go:396` (inside `resolve`)
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: truthful versions from Tasks 1–4.
- Produces: `gs.ready` persists between rounds, updated per key from the verdict's versions.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/gate/gate_test.go`:

```go
// The bug this whole plan exists for. A participant whose half is already
// correct submits once, gets a round that fails for the OTHER participant's
// reason, and stops — correctly, because it has nothing to fix. Clearing
// readiness after every round then made the peer's corrected re-submission
// unable to ever reach quorum again.
//
// billing submits exactly once here. If you find yourself making it re-submit
// to get this passing, the test has stopped testing the bug.
func TestStandingReadinessLetsAPeerReSubmitAlone(t *testing.T) {
	var rounds int32
	h := setupGate(t, checkoutSpec(), func(gateID string, versions map[string]string) gate.Verdict {
		// Fail while gateway is at g1; pass once it moves to g2.
		if versions["gateway"] == "g2" {
			return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
		}
		atomic.AddInt32(&rounds, 1)
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "gateway is wrong", Versions: versions}
	})
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")

	first := recvVerdict(t, h.verdict)
	if first.Passed {
		t.Fatalf("first round = %+v, want a failure", first)
	}

	// billing does NOT submit again — its half was correct all along.
	h.ready(t, "gateway", "g2")

	second := recvVerdict(t, h.verdict)
	if !second.Passed {
		t.Fatalf("second round = %+v, want a pass: billing's standing readiness should still count", second)
	}
	if second.Versions["billing"] != "b1" {
		t.Errorf("versions = %v, want billing's standing claim b1 carried into the round", second.Versions)
	}
}

// Standing readiness must not make the gate re-run work it has already done. A
// participant resubmitting a version the last round already tested is answered
// from the remembered verdict, and the runner is not invoked again. Asserting
// on the invocation count rather than the verdict is deliberate: a cache that
// has stopped working still produces correct verdicts, just expensively.
func TestStandingReadinessDoesNotOpenARedundantRound(t *testing.T) {
	var runs int32
	h := setupGate(t, checkoutSpec(), func(gateID string, versions map[string]string) gate.Verdict {
		atomic.AddInt32(&runs, 1)
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	recvVerdict(t, h.verdict)

	// Unchanged resubmit: must be answered from the remembered verdict.
	h.ready(t, "gateway", "g1")
	recvVerdict(t, h.verdict)

	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("runner invoked %d times, want 1: the unchanged resubmit should be answered from cache", got)
	}
}

// F2, replayed under standing readiness. Sticky invalidation is what keeps this
// design sound: without it, an unchanged resubmit would hit the cache and be
// answered with a verdict for a combination that no longer holds, because the
// other participant's standing claim moved underneath it.
func TestStandingReadinessStillInvalidatesOnDivergence(t *testing.T) {
	var lastTested map[string]string
	h := setupGate(t, checkoutSpec(), func(gateID string, versions map[string]string) gate.Verdict {
		lastTested = versions
		passed := versions["billing"] == "b2"
		return gate.Verdict{GateID: gateID, Passed: passed, Versions: versions}
	})
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	if v := recvVerdict(t, h.verdict); v.Passed {
		t.Fatalf("first round = %+v, want a failure", v)
	}

	// billing fixes its half. gateway then resubmits UNCHANGED — it must not be
	// answered from the pre-fix verdict.
	h.ready(t, "billing", "b2")
	second := recvVerdict(t, h.verdict)
	if !second.Passed {
		t.Fatalf("second round = %+v, want a pass once billing moved to b2", second)
	}
	if lastTested["billing"] != "b2" || lastTested["gateway"] != "g1" {
		t.Fatalf("runner saw %v, want billing=b2 with gateway's standing g1", lastTested)
	}
}
```

`sync/atomic` is needed in this test file's imports if not already present.

- [ ] **Step 2: Run the tests to verify the first fails**

Run: `go test ./pkg/gate/ -run TestStandingReadiness -v`
Expected: `TestStandingReadinessLetsAPeerReSubmitAlone` FAILS at `timed out waiting for verdict` — `gateway`'s second submission cannot reach quorum because `resolve` cleared `billing`'s readiness. That timeout IS the deadlock this plan fixes; confirm you see it before implementing.

- [ ] **Step 3: Make readiness standing**

In `pkg/gate/gate.go`'s `resolve`, replace the clearing line:

```go
	gs.ready = make(map[string]string) // re-arm for the next round
```

with:

```go
	// Readiness is a standing claim about a version — "my half is ready at X" —
	// not an event this round consumes. Clearing it here meant a participant
	// whose half was already correct starved the gate permanently by finishing
	// and exiting: its peer's corrected re-submission could never reach quorum
	// again. A live run lost exactly that way, with billing submitting once,
	// the round failing for gateway's reason, and billing correctly concluding
	// it had nothing to fix.
	//
	// Updated per key rather than replaced wholesale: each participant the
	// verdict names has its claim overwritten with what was actually tested,
	// and any participant the verdict does not name keeps the claim it had. The
	// key sets agree in practice — Config.validate requires every gate.required
	// name to appear in agents, and the runner reports one entry per merged
	// branch — but a per-key merge means a verdict that ever named a subset
	// could not silently erase the rest of the quorum.
	for name, tested := range v.Versions {
		gs.ready[name] = tested
	}
```

Note this runs after the backfill at `gate.go:393`, so on the compatibility path `v.Versions` is a copy of `gs.ready` and the loop is a no-op — standing claims are left exactly as they were.

- [ ] **Step 4: Run the tests**

Run: `go test ./pkg/gate/ -v -count=1`
Expected: PASS, all three new tests and every existing one. Pay attention to `TestPartialReadinessDoesNotOpen` and `TestDuplicateReadyLastVersionWins` — if either breaks, the quorum rule has changed in a way this plan did not intend, and that is a stop-and-report signal rather than a test to adjust.

Then `go test ./... -count=1` and `go test ./pkg/gate/ -count=5` — this package is timing-sensitive and a change to the round model is exactly what makes it flaky.

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): readiness is a standing claim, not a per-round event

A participant whose half is already correct starved the gate permanently by
submitting once and exiting: resolve cleared gs.ready after every round, so its
peer's corrected re-submission could never reach quorum again. A live run lost
exactly that way and needed a kill.

Updated per key from the verdict's versions rather than replaced, so a verdict
naming a subset could not erase the rest of the quorum. F2's sticky
invalidation turns out to be a prerequisite for this rather than just a past
repair: without it an unchanged resubmit would be answered from a verdict for a
combination that no longer holds."
```

---

## Task 6: `Submit` reports a version mismatch instead of hanging

**Files:**
- Modify: `internal/pcops/submit.go:21` (sentinels), `internal/pcops/submit.go:100` (the guard)
- Test: `internal/pcops/submit_test.go`

**Interfaces:**
- Consumes: truthful verdicts from Tasks 1–4.
- Produces: `var ErrVersionMismatch = errors.New("pcops: the gate tested a different version than was declared")`.

- [ ] **Step 1: Write the failing test**

Add to `internal/pcops/submit_test.go`:

```go
// Truthful verdicts mean an agent that commits again after submitting sees a
// verdict naming the sha actually merged, and the version guard correctly
// declines it. Nothing then wakes it: the post-ack select waits only on
// verdicts and ctx, so it burns the full SubmitTimeout and reports ErrNoVerdict
// — the F4 failure class this project spent a phase eliminating.
//
// The elapsed-time assertion is the point. With a generous deadline, "returns
// an error eventually" passes against exactly the hang this exists to prevent.
func TestSubmitReportsWhenTheGateTestedADifferentVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 45 * time.Second,
	}
	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	// The runner reports a sha that is NOT what billing declares below.
	gate.ServeRunner(run, func(_ context.Context, gateID string, _ map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true, Versions: map[string]string{"billing": "actually-merged"}}
	})
	go run.Run(ctx)

	start := time.Now()
	_, err = pcops.Submit(ctx, cfg, "g", "billing", "declared")
	elapsed := time.Since(start)

	if !errors.Is(err, pcops.ErrVersionMismatch) {
		t.Fatalf("Submit err = %v, want ErrVersionMismatch", err)
	}
	// Must return promptly, not at SubmitTimeout. AckTimeout is 10s and the
	// round resolves in well under that.
	if elapsed > 30*time.Second {
		t.Fatalf("Submit took %v to report a version mismatch; it waited out its budget instead of reporting", elapsed)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/pcops/ -run TestSubmitReportsWhenTheGateTestedADifferentVersion -v`
Expected: FAIL to compile — `undefined: pcops.ErrVersionMismatch`. Once the sentinel exists but the arm does not, it fails on the 45-second `SubmitTimeout` with `ErrNoVerdict`, which is the hang being fixed. Capture both phases.

- [ ] **Step 3: Add the sentinel**

In `internal/pcops/submit.go`, beside the other sentinels:

```go
// ErrVersionMismatch means a verdict arrived for this gate that named THIS
// agent at a version other than the one declared — the agent's branch moved
// after it submitted, so the gate merged and tested a later commit.
//
// It is actionable and must stay distinct from ErrNoVerdict: the gate is alive
// and working, and the right response is to resubmit at the current HEAD rather
// than to investigate a missing coordinator. Without this outcome the agent
// waits out its whole SubmitTimeout in silence, which is the F4 failure class
// (see docs/superpowers/specs/2026-09-02-live-fire-findings.md).
var ErrVersionMismatch = errors.New("pcops: the gate tested a different version than was declared")
```

- [ ] **Step 4: Signal it from the guard**

The `IntentInform` handler's guard currently calls `w.offerDeclined()` and returns for any non-matching verdict. It must distinguish two cases: a verdict that does not name this agent at all (a foreign round — keep declining silently), and one that names this agent at a different version (actionable).

Add a channel to `submitWaiter` in `internal/pcops/submitwaiter.go`, beside the others:

```go
	// mismatched carries the version a verdict said it tested for THIS agent
	// when that differs from the one declared. Distinct from declined: a
	// verdict that does not name us at all is someone else's round and we keep
	// waiting, but one that names us at a different version means our branch
	// moved and waiting is futile.
	mismatched chan string
```

Initialise it in `newSubmitWaiter` alongside the others with `make(chan string, 1)`, add it to `declareReady`'s drain loop, and add an offer method beside the others:

```go
func (w *submitWaiter) offerMismatch(tested string) {
	select {
	case w.mismatched <- tested:
	default:
	}
}
```

Then in `submit.go`'s guard:

```go
		versions := protocol.Versions(m.Body["versions"])
		if versions[agentName] != version {
			if tested, named := versions[agentName]; named {
				// The gate tested US, at a different commit than we declared —
				// our branch moved after we submitted. Actionable, and distinct
				// from a foreign round.
				w.offerMismatch(tested)
				return nil
			}
			// This verdict resolved the in-flight round that displaced our own
			// readiness (see the Nack handler below) — it is proof that round
			// is done, which is exactly what waitForRoundToResolve is waiting
			// for, so a re-declared readiness can now succeed.
			w.offerDeclined()
			return nil
		}
```

Finally add an arm to **both** selects in the attempt loop, beside the existing `case v := <-w.verdicts:`:

```go
		case tested := <-w.mismatched:
			return gate.Verdict{}, fmt.Errorf("%w: gate %q tested %q, you declared %q — resubmit at your current HEAD",
				ErrVersionMismatch, gateID, tested, version)
```

- [ ] **Step 5: Run the tests**

Run: `go test ./internal/pcops/ -run TestSubmit -v -count=1`
Expected: PASS, including every existing `TestSubmit*`. `TestSubmitDeclinesAVerdictThatDidNotIncludeIt` is the one to watch — it publishes a verdict naming a *different* agent's version, so it must still take the silent-decline path and must not become a mismatch.

Then `go test ./... -count=1`, `-race -count=1`, and `go test ./internal/pcops/ -count=3`.

- [ ] **Step 6: Commit**

```bash
git add internal/pcops/submit.go internal/pcops/submitwaiter.go internal/pcops/submit_test.go
git commit -m "feat(pcops): report a version mismatch instead of waiting out the budget

Truthful verdicts mean an agent that commits after submitting sees a verdict
naming the sha actually merged, and the guard declines it — but nothing woke
it, so it burned the full SubmitTimeout and reported ErrNoVerdict. Without this
outcome the design trades a deadlock for a timeout, which is the F4 failure
class again.

Distinct from declined: a verdict that does not name us is someone else's round
and we keep waiting; one that names us at a different version is actionable."
```

---

## Task 7: The acknowledgement says whether a round started

**Files:**
- Modify: `pkg/gate/gate.go:283` (the Ack body), `internal/pcops/watch.go`
- Test: `pkg/gate/gate_test.go`, `internal/pcops/watch_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: the Ack body gains `"running"` (bool).

- [ ] **Step 1: Write the failing tests**

Add to `pkg/gate/gate_test.go`:

```go
// An empty outstanding list means two different things — "you completed quorum
// and a round is starting" and "nothing to report" — and under standing
// readiness the first becomes the common case. Both pc submit and pc watch
// render it as silence, so the ack carries whether a round actually opened.
func TestAckSaysWhetherARoundStarted(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	first := recvMsg(t, h.acks)
	if running, _ := first.Body["running"].(bool); running {
		t.Errorf("ack for a partial quorum says a round is running: %+v", first.Body)
	}

	h.ready(t, "gateway", "g1")
	second := recvMsg(t, h.acks)
	if running, _ := second.Body["running"].(bool); !running {
		t.Errorf("ack for the quorum-completing submit does not say a round started: %+v", second.Body)
	}
}
```

`recvMsg(t, h.acks)` and the `h.acks` channel both already exist — `setupGate` feeds that channel at `gate_test.go:236`, and several existing tests read it the same way. Do not add a new helper for this.

Add to `internal/pcops/watch_test.go`:

```go
// Under standing readiness an ack usually carries an empty outstanding list,
// which rendered as a bare "coordinator → billing ack" and told an operator
// nothing. Say that the round started.
func TestFormatRecordShowsARoundStarting(t *testing.T) {
	m := protocol.New(protocol.Address{Agent: "coordinator"}, protocol.Address{Agent: "billing"},
		protocol.IntentAck, map[string]any{"gate": "g", "running": true})
	got := pcops.FormatRecord(sqlite.Record{Seq: 1, Msg: m}, false)
	if !strings.Contains(got, "round running") {
		t.Fatalf("rendered %q, want it to say the round is running", got)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./pkg/gate/ -run TestAckSays -v` and `go test ./internal/pcops/ -run TestFormatRecordShowsARoundStarting -v`
Expected: both FAIL — the gate one on the quorum-completing ack, the watch one with a rendering that lacks the phrase.

- [ ] **Step 3: Add the flag to the Ack**

In `pkg/gate/gate.go`'s `onReady`, `full` is already computed above the Ack. Add it to the body:

```go
		outstanding := outstandingFor(gs)
		ack := m.Reply(protocol.Address{Agent: c.a.Name}, protocol.IntentAck, map[string]any{
			"gate":        gateID,
			"outstanding": outstanding,
			// Under standing readiness an empty outstanding list is the common
			// case — once everyone has submitted once, any later submit
			// completes the set immediately — so "nothing outstanding" and "a
			// round just started" became indistinguishable, and both pc submit
			// and pc watch rendered them as silence.
			"running": full,
		})
```

- [ ] **Step 4: Render it in `watch`**

In `internal/pcops/watch.go`'s `summarise`, the `protocol.IntentAck` case currently returns `"waiting on " + ...` only when the outstanding list is non-empty. Extend it:

```go
	case protocol.IntentAck:
		if out := protocol.Strings(m.Body["outstanding"]); len(out) > 0 {
			return "waiting on " + strings.Join(out, ", ")
		}
		if running, _ := m.Body["running"].(bool); running {
			return "round running"
		}
```

- [ ] **Step 5: Run the tests**

Run: `go test ./pkg/gate/ ./internal/pcops/ -count=1` then `go test ./... -count=1`.
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go internal/pcops/watch.go internal/pcops/watch_test.go
git commit -m "feat(gate,pcops): the ack says whether a round started

Under standing readiness an empty outstanding list is the common case, so
'nothing outstanding' and 'a round just started' became indistinguishable and
both pc submit and pc watch rendered them as silence. full is already computed
on that line."
```

---

## Final verification

Run all of these before considering the plan complete:

- `gofmt -l pkg internal cmd fixtures` — only `cmd/sqlitedemo/main.go`.
- `go vet ./...` and `go build ./...` — clean.
- `go test ./... -count=1` — all pass.
- `go test ./... -race -count=1` — clean.
- `go test ./pkg/gate/ ./internal/pcops/ -count=5` — deterministic. The round model changed; this is where flakiness would show.
- `go list ./... | grep -c twoservice` — 0; the fixture stays outside the module.
- `go run ./cmd/pc` — usage lists six commands.

**Not verified by any of the above:** whether a real two-agent run now converges where it previously deadlocked. That needs `scripts/live-run.sh` with two coding agents and a human watching, and it is the actual proof this plan exists for. Run it after the branch is green — and never without `--preflight-only` unless you intend to spend money.
