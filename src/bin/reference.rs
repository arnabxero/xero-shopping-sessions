use std::path::PathBuf;

fn main() {
    let arguments: Vec<String> = std::env::args().collect();
    let pick = |at: usize, name: &str| {
        if arguments.len() >= 3 {
            PathBuf::from(&arguments[arguments.len() - 2 + at])
        } else {
            PathBuf::from(std::env::var(name).unwrap())
        }
    };
    nb7x::slow::run(&pick(0, "DATA_DIR"), &pick(1, "OUTPUT_DIR"));
}
