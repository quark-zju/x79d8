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
    if let Err(e) = opt.run().await {
        eprintln!("Error: {} ({:?})", &e, &e)
    }
}

fn init() {
    env_logger::Builder::from_env("X79D8_LOG")
        .format_timestamp_millis()
        .init();
}
