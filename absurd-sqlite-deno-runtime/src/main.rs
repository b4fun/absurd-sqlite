mod deno_rt;

fn main() {
    let script_path = match std::env::args().nth(1) {
        Some(path) => path,
        None => {
            eprintln!("usage: absurd-deno-runtime <script>");
            std::process::exit(1);
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(deno_rt::run(&script_path)).unwrap();
}
