mod csv;
mod huge;
mod links;
mod pack;
mod parquet_writer;
mod pipeline;

use std::env;
use std::path::PathBuf;

fn main() {
    let default_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let num_threads = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default_threads)
        .clamp(1, 64);

    let args: Vec<String> = env::args().skip(1).collect();
    let data_dir = if args.len() >= 2 {
        PathBuf::from(&args[0])
    } else {
        PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string()))
    };
    let out_dir = if args.len() >= 2 {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string()))
    };

    pipeline::run_pipeline(&data_dir, &out_dir, num_threads);
    std::process::exit(0);
}
