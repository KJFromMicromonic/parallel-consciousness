# Other harnesses

There are no recipes here for other harnesses, deliberately. This project
claims support only for what it has driven end to end through a real gate,
which today is pi and Claude Code.

## What a harness needs

Any coding agent can participate if it can do four things:

1. **Run a shell command** and let the agent see its output. `pc submit` and
   `pc send` are shell commands; nothing else is required.
2. **Surface exit codes**, or at least let the agent read them. `pc submit`
   distinguishes pass, fail and could-not-decide by exit status.
3. **Accept an instruction file** at startup — `docs/agent-contract.md`, by
   whatever mechanism the harness offers.
4. **Work in a directory you choose**, so it can be pointed at a git worktree.

No MCP server, no plugin, and no API integration is needed.

## If you get one working

The two existing recipes are short because there was little to say once the
contract loaded. If you drive a third harness through a full run, a recipe of
the same shape is welcome — and please record what surprised you, not just what
worked. Both existing recipes carry a trap that cost real time.
