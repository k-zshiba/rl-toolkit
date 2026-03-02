# rl-toolkit

This toolkit includes utility tools for rocket league below

## rl-replay-harvester

A CLI tool that queries ballchasing API and downloads replay files for a specified pro player.

### Usage

```bash
export BALLCHASING_API_KEY="your_api_key"
export BALLCHASING_REQUEST_SLEEP_MS="1000" # optional
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

## Windows Build

Build only the two tool binaries for Windows (`x86_64-pc-windows-gnu`) using `cross`:

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
  -p rl-replay-harvester \
  -p rl-replay2json
```
