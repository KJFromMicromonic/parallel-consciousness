# two-service fixture

Two services that must agree on one value, and a spanning test that only the
gate can run. It exists to be driven by real coding agents through
`pc up` / `pc submit`, not by CI — `scripts/live-run.sh` is its usual driver.

It is a separate Go module so the parent project's `go build ./...`,
`go vet ./...` and `go test ./...` do not see it. Agents mutate it freely
during a run; nothing here is project source.

## The gate command

    EXPECTED_CURRENCY=USD go test ./integration/...

## Four constraints, each learned the hard way

1. **Each half compiles independently.** `billing.Invoice.Currency` exists up
   front. An earlier version made adding the field one agent's job and setting
   it the other's, so one could not compile until the other merged, and they
   deadlocked instead of coordinating.
2. **The agreed value is undiscoverable from either worktree.** `USD` appears
   nowhere in this module. It reaches the run only through the gate command's
   environment. Without this, capable models read the expectation out of the
   test and the failure branch is never exercised.
3. **One side is seeded wrong.** `gateway` stamps `"EUR"`, so round one fails
   for a reason agents actually encounter: inherited code that looks fine.
4. **The spanning test skips when the variable is absent**, so an agent running
   the suite locally is not misled, and the contract's claim that it cannot run
   the spanning test itself stays true.

`internal/fixtures/twoservice_test.go` in the parent module enforces all four.
