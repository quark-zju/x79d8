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
    let env = std::env::var("X79D8_LOG");
    let filter_str = match env.as_ref() {
        Ok(s) => s.as_str(),
        Err(_) => "x79d8=info",
    };
    env_logger::Builder::new()
        .parse_filters(filter_str)
        .format_timestamp_millis()
        .init();
}
