use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use datasketches::hll::{HllSketch, HllType};

const MODE_BYTE: usize = 7;
const CUR_MODE_MASK: u8 = 0x03;

const CUR_MODE_LIST: u8 = 0;
const CUR_MODE_SET: u8 = 1;
const CUR_MODE_HLL: u8 = 2;

struct FixtureSpec {
    file_name: &'static str,
    mode_name: &'static str,
    sketch: HllSketch,
    inserted_count: u64,
    tolerance_abs: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = parse_out_dir(env::args().skip(1))?;
    fs::create_dir_all(&out_dir)?;

    let fixtures = vec![
        FixtureSpec {
            file_name: "empty_lgk12_hll8.bin",
            mode_name: "EMPTY",
            sketch: HllSketch::new(12, HllType::Hll8),
            inserted_count: 0,
            tolerance_abs: 0,
        },
        {
            let (sketch, inserted_count) = build_sketch_for_mode(12, HllType::Hll8, CUR_MODE_LIST);
            FixtureSpec {
                file_name: "list_lgk12_hll8.bin",
                mode_name: "LIST",
                sketch,
                inserted_count,
                tolerance_abs: 0,
            }
        },
        {
            let (sketch, inserted_count) = build_sketch_for_mode(12, HllType::Hll8, CUR_MODE_SET);
            FixtureSpec {
                file_name: "set_lgk12_hll8.bin",
                mode_name: "SET",
                sketch,
                inserted_count,
                tolerance_abs: 2,
            }
        },
        {
            let (sketch, inserted_count) = build_sketch_for_mode(12, HllType::Hll8, CUR_MODE_HLL);
            FixtureSpec {
                file_name: "hll_lgk12_hll8.bin",
                mode_name: "HLL",
                sketch,
                inserted_count,
                tolerance_abs: 8,
            }
        },
        fixed_count_fixture("hll_lgk12_hll8_n1000.bin", 12, HllType::Hll8, 1_000, 32),
        fixed_count_fixture("hll_lgk12_hll8_n10000.bin", 12, HllType::Hll8, 10_000, 320),
        fixed_count_fixture("hll_lgk12_hll8_n100000.bin", 12, HllType::Hll8, 100_000, 3_200),
        fixed_count_fixture("hll_lgk12_hll8_n1000000.bin", 12, HllType::Hll8, 1_000_000, 32_000),
    ];

    let mut manifest =
        String::from("file_name\tmode\tinserted_count\ttolerance_abs\n");
    for fixture in fixtures {
        write_fixture(&out_dir, fixture.file_name, fixture.sketch)?;
        manifest.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            fixture.file_name, fixture.mode_name, fixture.inserted_count, fixture.tolerance_abs
        ));
    }
    fs::write(out_dir.join("manifest.tsv"), manifest)?;

    eprintln!("wrote fixtures to {}", out_dir.display());
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
                let value = args
                    .next()
                    .ok_or("missing value after --out-dir")?;
                out_dir = PathBuf::from(value);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
    }

    Ok(out_dir)
}

fn print_usage() {
    eprintln!("Usage: cargo run --bin generate_apache_hll_fixtures -- [--out-dir PATH]");
}

fn write_fixture(
    out_dir: &Path,
    file_name: &str,
    sketch: HllSketch,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = sketch.serialize();
    fs::write(out_dir.join(file_name), bytes)?;
    Ok(())
}

fn build_sketch_for_mode(lg_k: u8, hll_type: HllType, target_mode: u8) -> (HllSketch, u64) {
    let mut sketch = HllSketch::new(lg_k, hll_type);

    for value in 0u64.. {
        sketch.update(value);
        if current_mode(&sketch.serialize()) == Some(target_mode) {
            return (sketch, value + 1);
        }
    }

    unreachable!("monotonic mode promotion should eventually reach target mode");
}

fn fixed_count_fixture(
    file_name: &'static str,
    lg_k: u8,
    hll_type: HllType,
    inserted_count: u64,
    tolerance_abs: u64,
) -> FixtureSpec {
    FixtureSpec {
        file_name,
        mode_name: "HLL",
        sketch: build_sketch_with_count(lg_k, hll_type, inserted_count),
        inserted_count,
        tolerance_abs,
    }
}

fn build_sketch_with_count(lg_k: u8, hll_type: HllType, inserted_count: u64) -> HllSketch {
    let mut sketch = HllSketch::new(lg_k, hll_type);
    for value in 0..inserted_count {
        sketch.update(value);
    }
    sketch
}

fn current_mode(bytes: &[u8]) -> Option<u8> {
    bytes.get(MODE_BYTE).map(|mode| mode & CUR_MODE_MASK)
}
