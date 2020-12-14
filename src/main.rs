use cli::Opt;
use structopt::StructOpt;

mod cli;
mod ftpfs;
mod intkv;
mod util;

#[tokio::main]
pub async fn main() {
    init();
    let opt = Opt::from_args();
    match opt.run().await {
        Err(e) => eprintln!("Error: {}", e),
        Ok(_) => {}
    }
}

fn init() {
    env_logger::Builder::from_env("X79D8_LOG")
        .format_timestamp_millis()
        .init();
}
