use crate::{
    ftpfs::IntKvFtpFs,
    intkv::{
        backend::FsIntKv,
        wrapper::{BufferedIntKv, EncIntKv, PageIntKv},
        IntKv,
    },
};
use scrypt::Params as ScryptParams;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
#[derive(Debug, StructOpt)]
#[structopt(name = "x79d8", about = "Serve encrypted files via local FTP.")]
pub(crate) enum Opt {
    /// Initializes a directory to store encrypted data.
    Init {
        /// Block size in KB. Blocks hide individual file size information.
        /// 0: Disable blocks (do not hide file size information).
        #[structopt(short, long, default_value = "1024")]
        block_size_kb: u16,

        /// Disable encryption.
        #[structopt(long)]
        no_encrypt: bool,

        /// Log 2 of the scrypt parameter N. Affects memory and CPU.
        #[structopt(long, default_value = "15")]
        scrypt_log_n: u8,

        /// Path to the local directory.
        #[structopt(name = "DIR", default_value = ".")]
        dir: PathBuf,
    },

    /// Serves an encrypted directory.
    Serve {
        /// FTP service address.
        #[structopt(short, long, default_value = "127.0.0.1:7968")]
        address: String,

        /// Path to the local directory.
        #[structopt(name = "DIR", default_value = ".")]
        dir: PathBuf,
    },
}

static CONFIG_FILE: &str = "x79d8cfg.json";

const fn default_cache_size_limit() -> usize {
    1 << 28
}

const fn default_scrypt_log_n() -> u8 {
    15
}

const fn default_scrypt_r() -> u32 {
    8
}

const fn default_scrypt_p() -> u32 {
    1
}

const fn default_block_size_kb() -> u16 {
    1024
}

#[derive(Debug, StructOpt, Serialize, Deserialize)]
struct Config {
    pub salt_hex: String,
    #[serde(default = "default_block_size_kb")]
    pub block_size_kb: u16,
    #[serde(default = "default_scrypt_log_n")]
    pub scrypt_log_n: u8,
    #[serde(default = "default_scrypt_r")]
    pub scrypt_r: u32,
    #[serde(default = "default_scrypt_p")]
    pub scrypt_p: u32,
    #[serde(default = "default_cache_size_limit")]
    pub cache_size_limit: usize,
}

impl Opt {
    /// Run the command.
    pub async fn run(&self) -> io::Result<()> {
        match self {
            Opt::Init {
                block_size_kb,
                no_encrypt,
                scrypt_log_n,
                dir,
            } => init_cmd(dir, *block_size_kb, !no_encrypt, *scrypt_log_n),
            Opt::Serve { address, dir } => serve_cmd(dir, address).await,
        }
    }
}

fn init_cmd(dir: &Path, block_size_kb: u16, encrypted: bool, scrypt_log_n: u8) -> io::Result<()> {
    let dir = fs::canonicalize(dir)?;
    let config_path = dir.join(CONFIG_FILE);
    if config_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} was already initialized", dir.display()),
        ));
    }
    let config = {
        let salt_hex = if encrypted {
            let salt: [u8; 32] = rand::random();
            hex::encode(salt)
        } else {
            String::new()
        };
        Config {
            salt_hex,
            scrypt_log_n,
            scrypt_r: default_scrypt_r(),
            scrypt_p: default_scrypt_p(),
            block_size_kb,
            cache_size_limit: default_cache_size_limit(),
        }
    };
    fs::write(
        config_path,
        serde_json::to_string_pretty(&config).unwrap().as_bytes(),
    )?;

    eprintln!("Initialized {}", dir.display());
    Ok(())
}

async fn serve_cmd(dir: &Path, address: &str) -> io::Result<()> {
    let dir = fs::canonicalize(dir)?;
    let kv = kv_from_dir(&dir)?;
    let fs = IntKvFtpFs::new(kv);
    tokio::task::spawn(flush_on_ctrl_c(fs.clone()));

    let logger = slog::Logger::root(slog::Drain::ignore_res(slog_stdlog::StdLog), slog::o!());
    let server = libunftp::Server::new(Box::new(move || fs.clone()))
        .greeting("x79db server")
        .passive_ports(50000..65535)
        .logger(logger);

    eprintln!("Serving {} at ftp://{}", dir.display(), address);
    server
        .listen(address)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok(())
}

async fn flush_on_ctrl_c(mut fs: IntKvFtpFs) {
    while tokio::signal::ctrl_c().await.is_ok() {
        eprintln!("Writing changes on Ctrl+C...");
        match fs.flush() {
            Ok(_) => {
                eprintln!("Done. Exiting.");
                std::process::exit(0);
            }
            Err(e) => eprintln!("Failed: {}", e),
        }
    }
}

/// Construct the `IntKv` backend.
fn kv_from_dir(dir: &Path) -> io::Result<Box<dyn IntKv>> {
    let config_path = dir.join(CONFIG_FILE);
    if !config_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} was not initialized (try \"x79d8 init\")", dir.display()),
        ));
    }

    let config: Config = {
        let config_str = fs::read_to_string(config_path)?;
        serde_json::from_str(&config_str)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
    };

    kv_from_dir_config(dir, &config)
}

/// Construct the `IntKv` backend.
fn kv_from_dir_config(dir: &Path, config: &Config) -> io::Result<Box<dyn IntKv>> {
    let mut kv: Box<dyn IntKv> = { Box::new(FsIntKv::new(dir)?) };
    let mut page_overhead = 0;
    if config.salt_hex.is_empty() {
        log::info!("Encryption is disabled");
    } else {
        let prompt = "Password: ";
        let pass = rpassword::read_password_from_tty(Some(prompt)).unwrap();
        let key = password_derive(&pass, config);
        // Use password encryption.
        kv = Box::new(EncIntKv::from_key_kv(key, kv));
        // Bytes per page is used by encryption header (IV count).
        page_overhead = EncIntKv::iv_header_size() as u64;
    }

    kv = Box::new(BufferedIntKv::new(kv).with_cache_size_limit(config.cache_size_limit));
    if config.block_size_kb > 0 {
        let block_size = (config.block_size_kb as u64) * 1024;
        kv = Box::new(PageIntKv::new(block_size - page_overhead, kv)?);
    }
    Ok(kv)
}

/// Derive key from password.
fn password_derive(password: &str, config: &Config) -> [u8; 32] {
    let params = ScryptParams::recommended();
    let salt = hex::decode(&config.salt_hex).unwrap();
    let mut output = [0u8; 32];
    scrypt::scrypt(password.as_bytes(), &salt, &params, &mut output).unwrap();
    output
}
