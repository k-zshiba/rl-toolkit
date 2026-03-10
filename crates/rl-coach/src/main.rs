use anyhow::Result;
use clap::Parser;
use rl_coach::analyze_path;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "rl-coach")]
#[command(
    about = "Analyze Rocket League replay JSON exported by rl-replay2json",
    version
)]
struct Args {
    #[arg(long = "input", short = 'i', value_name = "FILE_OR_DIR")]
    input: PathBuf,
    #[arg(long = "output-dir", short = 'o', value_name = "DIR")]
    output_dir: PathBuf,
    #[arg(long = "pretty-json")]
    pretty_json: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let summary = analyze_path(&args.input, &args.output_dir, args.pretty_json)?;

    for manifest in &summary.matches {
        println!(
            "{} {} {}-{} {} {}",
            manifest.date,
            manifest.replay_id,
            manifest.final_score.blue,
            manifest.final_score.orange,
            manifest
                .winner
                .clone()
                .unwrap_or_else(|| "draw".to_string()),
            manifest.report_path
        );
    }

    if args.input.is_dir() {
        println!(
            "summary {}",
            args.output_dir
                .join("analysis")
                .join("summary.json")
                .display()
        );
    }

    Ok(())
}
