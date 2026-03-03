# rl-toolkit 日本語ガイド

このリポジトリは、Rocket League のリプレイ収集と変換を行うツール群です。

## rl-common (GUI)

`rl-common` クレートは GUI アプリです。生成されるバイナリ名は `rl-toolkit`（Windows では `rl-toolkit.exe`）です。

### 起動方法

```bash
cargo run -p rl-common
```

### 主な機能

- Replay Harvester タブ
  - Ballchasing API を使って指定プレイヤーのリプレイをダウンロード
  - `Output Dir` は `Browse...` ボタンでフォルダ選択可能
  - リクエスト間隔は秒単位（最小 2 秒）
- Replay2JSON タブ
  - `.replay` を JSON に変換
  - `Input Dir` / `Output Dir` は `Browse...` ボタンでフォルダ選択可能
  - 監視モードで追加ファイルを継続変換可能

### 補足

- Windows で動作します。
- Linux では `WINIT_UNIX_BACKEND=x11` をデフォルト設定し、Wayland の `XKBNotFound` 問題を回避します。

## rl-replay-harvester (CLI)

Ballchasing API から指定選手のリプレイをダウンロードする CLI ツールです。

### 実行例

```bash
export BALLCHASING_API_KEY="your_api_key"
export BALLCHASING_REQUEST_INTERVAL_SECONDS="2" # 任意、最小 2 秒
cargo run -p rl-replay-harvester -- "Zen" --output-dir /path/to/output
```

### 引数

- `player`（必須）: 検索するプレイヤー名
- `--output-dir`, `-o`（必須）: 保存先のベースディレクトリ

### 保存形式

```text
replays/{player_slug}/{yyyy-mm-dd}/{replay_id}.replay
```

- `player_slug`: プレイヤー名をファイル名向けに正規化した値
- 日付は API の `date` を優先し、なければ `created`、それもなければ現在 UTC 日付

## rl-replay2json (CLI)

`boxcars` を使って `.replay` を JSON に変換する CLI ツールです。

### 実行例

```bash
cargo run -p rl-replay2json -- \
  --input-dir /path/to/replays \
  --output-dir /path/to/output
```

### 引数

- `--input-dir`, `-i`（必須）: `.replay` を含むディレクトリ
- `--output-dir`, `-o`（必須）: JSON の出力先ベースディレクトリ

### 出力形式

```text
json/{yyyy-mm-dd}/{replay_filename}.json
```

- 入力ディレクトリは再帰的に探索
- 10 秒ごとに新規ファイルを検知して継続変換

## バージョン更新チェックと自己更新

以下のバイナリは起動時に GitHub Releases の latest を参照して、更新可能かを確認します。

- `rl-toolkit` (GUI)
- `rl-replay-harvester`
- `rl-replay2json`

新しいバージョンがある場合は、更新するかユーザーに確認します。

- CLI: 起動時に `[y/N]` プロンプト表示
- GUI: 起動時に Yes/No ダイアログ表示

ユーザーが更新を許可した場合:

- Linux: 実行ファイルをその場で置き換え
- Windows: 更新をステージングし、プロセス終了後に置き換え（再起動で反映）

利用できる環境変数:

- `RL_TOOLKIT_UPDATE_CHECK=off`（または `0` / `false`）で更新チェック無効化
- `RL_TOOLKIT_RELEASE_API_URL=<url>` で参照先 API を上書き
- `RL_TOOLKIT_GITHUB_TOKEN=<token>`（または `GITHUB_TOKEN`）で GitHub API 認証

デフォルト参照先:

`https://api.github.com/repos/k-zshiba/rl-toolkit/releases/latest`

## Windows 向けビルド

### スクリプト実行

```bash
./scripts/build-windows.sh
```

### 直接実行

```bash
cross build --release \
  --target x86_64-pc-windows-gnu \
  -p rl-common \
  -p rl-replay-harvester \
  -p rl-replay2json
```

### 生成物

- `target/x86_64-pc-windows-gnu/release/rl-toolkit.exe`
- `target/x86_64-pc-windows-gnu/release/rl-replay-harvester.exe`
- `target/x86_64-pc-windows-gnu/release/rl-replay2json.exe`
