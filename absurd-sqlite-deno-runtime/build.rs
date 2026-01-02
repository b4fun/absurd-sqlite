use std::env;
use std::fs;
use std::path::PathBuf;

use absurd_sqlite_deno_builder::{EszipBuilderOptions, build_eszip};
use tokio::runtime::Builder;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let deno_main_dir = manifest_dir.join("deno-main");
    println!("cargo:rerun-if-changed={}", deno_main_dir.display());

    let opts = EszipBuilderOptions::from_dir(deno_main_dir.clone())
        .expect("failed to construct eszip builder options");

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    let eszip = runtime
        .block_on(build_eszip(opts))
        .expect("failed to build eszip");
    let output_path = manifest_dir.join("deno-main.eszip2");
    fs::write(&output_path, eszip.into_bytes()).expect("failed to write eszip");
    println!("cargo:rerun-if-changed={}", output_path.display());
}
