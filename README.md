# rl-toolkit

This toolkit includes utility tools for rocket league below

Japanese guide: [README.ja.md](./README.ja.md)

## rl-common (GUI)

A desktop GUI application that provides both:
- replay harvesting via ballchasing API
- replay-to-json conversion via `boxcars`

### Usage

```bash
cargo run -p rl-common
```

Generated binary name: `rl-toolkit` (`rl-toolkit.exe` on Windows)

Notes:
- Windows is supported for this GUI app.
- On Linux, the app defaults `WINIT_UNIX_BACKEND=x11` to avoid common Wayland `XKBNotFound` issues.

### GUI features

- Replay Harvester tab:
  - API key / player / output directory input
  - `Browse...` button for output folder selection
  - request interval in seconds (minimum 2)
  - downloads replays to `replays/{player_slug}/{yyyy-mm-dd}/{replay_id}.replay`
- Replay2JSON tab:
  - input/output directory input
  - `Browse...` buttons for input/output folder selection
  - one-shot conversion or watch mode
  - writes JSON to `json/{yyyy-mm-dd}/{replay_filename}.json`

## rl-replay-harvester

A CLI tool that queries ballchasing API and downloads replay files for a specified pro player.

### Usage

```bash
export BALLCHASING_API_KEY="your_api_key"
export BALLCHASING_REQUEST_INTERVAL_SECONDS="2" # optional, minimum: 2
cargo run -p rl-replay-harvester -- "Zen" --output-dir /path/to/output
```

### Arguments

- `player` (required): player name to query on ballchasing
- `--output-dir`, `-o` (required): base directory to store downloaded replay files

### Replay file layout

The output format is:

`replays/{player_slug}/{yyyy-mm-dd}/{replay_id}.replay`

Notes:
- `player_slug` is a filesystem-safe normalized value from the input player name (lowercase and `_` separators)
- date uses replay `date` (fallback: API `created`, then current UTC date)
- request interval is controlled by `BALLCHASING_REQUEST_INTERVAL_SECONDS` (minimum 2 seconds)

This keeps partitions stable and easy to process for later JSON conversion and analytics jobs.

## rl-replay2json

A CLI tool that converts `.replay` files to JSON using `boxcars`.

### Usage

```bash
cargo run -p rl-replay2json -- \
  --input-dir /path/to/replays \
  --output-dir /path/to/output
```

### Arguments

- `--input-dir`, `-i` (required): directory that contains `.replay` files
- `--output-dir`, `-o` (required): base directory for converted JSON files

### Output layout

The output format is:

`json/{yyyy-mm-dd}/{replay_filename}.json`

Notes:
- the tool scans the input directory recursively
- `yyyy-mm-dd` is extracted from ancestor directory names when available (fallback: file modified date in UTC)
- output filename is the original replay filename with extension changed from `.replay` to `.json`
- the process keeps running and polls every 10 seconds
- only newly detected replay files are converted during runtime

## Version Update Check and Self-Update

All binaries check on startup whether a newer version exists on GitHub Releases:

- `rl-toolkit` (GUI)
- `rl-replay-harvester`
- `rl-replay2json`

If a newer version is found, the app asks the user whether to update.

- CLI tools: prompt on startup (`[y/N]`)
- GUI tool: Yes/No dialog on startup

If user agrees:

- Linux: executable is replaced immediately
- Windows: update is staged and applied after process exit (restart required)

Environment variables:

- `RL_TOOLKIT_UPDATE_CHECK=off` (or `0` / `false`) to disable update checks
- `RL_TOOLKIT_RELEASE_API_URL=<url>` to override release API endpoint
- `RL_TOOLKIT_GITHUB_TOKEN=<token>` (or `GITHUB_TOKEN`) for authenticated GitHub API requests

Default endpoint:

`https://api.github.com/repos/k-zshiba/rl-toolkit/releases/latest`

## Windows Build

Build GUI and CLI binaries for Windows (`x86_64-pc-windows-gnu`) using `cross`:

```bash
./scripts/build-windows.sh
```

You can also specify target and profile:

```bash
./scripts/build-windows.sh x86_64-pc-windows-gnu --release
./scripts/build-windows.sh x86_64-pc-windows-gnu
```

Direct command equivalent:

```bash
cross build --release \
  --target x86_64-pc-windows-gnu \
  -p rl-common \
  -p rl-replay-harvester \
  -p rl-replay2json
```
