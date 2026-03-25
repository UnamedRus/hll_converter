use std::env;
use std::fs;
use std::path::PathBuf;

use datasketches::hll::HllSketch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = parse_out_dir(env::args().skip(1))?;
    let manifest = fs::read_to_string(out_dir.join("manifest.tsv"))?;

    for line in manifest.lines().skip(1) {
        if line.trim().is_empty() {
            continue;
        }

        let mut cols = line.split('\t');
        let file_name = cols.next().ok_or("missing file_name")?;
        let mode = cols.next().ok_or("missing mode")?;
        let inserted_count = cols.next().ok_or("missing inserted_count")?;
        let tolerance_abs = cols.next().ok_or("missing tolerance_abs")?;

        let bytes = fs::read(out_dir.join(file_name))?;
        let sketch = HllSketch::deserialize(&bytes)?;

        println!(
            "{}\t{}\t{}\t{}\t{:.6}",
            file_name,
            mode,
            inserted_count,
            tolerance_abs,
            sketch.estimate()
        );
    }

    Ok(())
}

fn parse_out_dir<I>(mut args: I) -> Result<PathBuf, Box<dyn std::error::Error>>
where
    I: Iterator<Item = String>,
{
    let mut out_dir = PathBuf::from("testdata/apache_hll");

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out-dir" => {
                let value = args.next().ok_or("missing value after --out-dir")?;
                out_dir = PathBuf::from(value);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    Ok(out_dir)
}

fn print_usage() {
    eprintln!("Usage: cargo run --bin print_apache_hll_estimates -- [--out-dir PATH]");
}
