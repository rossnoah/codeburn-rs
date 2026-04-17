<h1 align="center">codeburn-rs</h1>

like codeburn, but in rust and a lot faster.

## Performance

Benchmarked against the published JS version (`npx codeburn`) with hyperfine
(20 runs / 10 warmup) on Apple Silicon. Command: `report --provider all --period week`.

| Scenario          | Rust (`cburn`) | JS (`npx codeburn`) | Speedup |
| ----------------- | -------------- | ------------------- | ------- |
| Tier 1 cache hit  | 6.5 ms         | 3.63 s              | ~557×   |
| Tier 2 cache hit  | 11.1 ms        | 4.33 s              | ~390×   |
| Cold (no cache)   | 162 ms         | 7.22 s              | ~44×    |

**Tier 1** is the output cache — replays the prebuilt rendered report.
**Tier 2** is the parse cache — skips the JSONL parse and classification step
but rebuilds the output. **Cold** wipes cursor disk caches and forces a full
SQLite scan through the parse pipeline.

### Data corpus on the test machine

| Provider       | Storage              | Size    | Notes                                |
| -------------- | -------------------- | ------- | ------------------------------------ |
| Claude Code    | 1,106 JSONL files    | 439 MB  | 71 projects, ~111k total lines       |
| Codex          | 247 JSONL files      | 98 MB   | ~43k total lines                     |
| Cursor         | SQLite (`state.vscdb`)| 1.5 GB | full IDE state — selective scan       |
| OpenCode       | SQLite (`opencode.db`)| 80 MB   | (1.1 GB including blobs/attachments) |
| Claude Desktop | session files        | 3.9 MB  |                                      |
| Pi             | session files        | 2.8 MB  |                                      |

The "Cold" run scans all of this from disk. The 154 ms total includes parsing
~150k JSONL lines plus a targeted SQLite query against the 1.5 GB Cursor DB.

## Install

### Homebrew

```sh
brew install rossnoah/tap/cburn
```

### Shell installer

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/rossnoah/codeburn-rs/releases/latest/download/cburn-installer.sh | sh
```

### From source

```sh
cargo install --git https://github.com/rossnoah/codeburn-rs
```

## Usage

Open the interactive dashboard:

```sh
cburn
```

Jump to a specific period:

```sh
cburn today
cburn month
cburn report --period 30days
```

Filter to one provider:

```sh
cburn report --provider cursor
```

Supported providers: `all`, `claude`, `codex`, `cursor`, `opencode`.

### Other commands

```sh
cburn status                    # compact terminal snapshot (today + week + month)
cburn export --format csv       # export usage data to CSV or JSON
cburn currency GBP              # change display currency
cburn install-menubar           # macOS menu bar widget (SwiftBar)
```

For full options, see `cburn --help` or `cburn <subcommand> --help`.

The binary is named `cburn` to avoid colliding with the npm `codeburn` package.
If you don't have the npm version installed and prefer the full name, alias it:

```sh
alias codeburn=cburn
```
