<h1 align="center">codeburn-rs</h1>

See where your AI coding tokens go but **600x faster**

Benchmarked against the published JS version (`npx codeburn`) with hyperfine on MacBook Pro (M1 Pro, 16GB, 1TB).

| Rust cache state | JS              | Rust (`cburn`) | JS (`npx codeburn`) | Speedup |
| ---------------- | --------------- | -------------- | ------------------- | ------- |
| cached output    | cached          | 6.0 ms         | 3.66 s              | ~610×   |
| cached sources   | cached          | 10.9 ms        | 3.66 s              | ~335×   |
| cold (bo cache)  | cold (no cache) | 76 ms          | 7.71 s              | ~101×   |

Supported providers: Claude, Codex, Opencode, Pi, Copilot.

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

### Other commands

```sh
cburn today                     # jump to today's usage
cburn month                     # jump to this month's usage
cburn report --period 30days    # report over a custom period
cburn report --provider claude  # filter to a single provider
cburn status                    # compact terminal snapshot (today + week + month)
cburn export --format csv       # export usage data to CSV or JSON
cburn currency GBP              # change display currency
```

For full options, see `cburn --help` or `cburn <subcommand> --help`. The binary is named `cburn` to avoid colliding with the npm `codeburn` package — if you don't have the npm version installed and prefer the full name, alias it.

```sh
alias codeburn=cburn
```

> Cursor support is currently disabled: Cursor stopped writing per-call token
> counts to its local `state.vscdb` in early 2026, so any parse of that DB
> now reports $0 regardless of actual usage. The parser code is retained in
> case the data layout is restored upstream.

Original JS version: (https://github.com/AgentSeal/codeburn).
