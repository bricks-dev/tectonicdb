#![feature(box_syntax, box_patterns, entry_and_modify)]

extern crate libtectonic;
extern crate clap;
extern crate byteorder;
extern crate chrono;
extern crate serde;
extern crate time;
#[macro_use]
extern crate serde_derive;
extern crate openssl_probe;
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;
extern crate stackdriver_logging_utils;

extern crate uuid;
extern crate circular_queue;

extern crate futures;
extern crate tokio_io;
extern crate tokio_core;
extern crate tokio_signal;

mod plugins;

mod server;
mod state;
mod utils;
mod parser;
mod handler;
mod settings;
mod subscription;

use clap::{Arg, App, ArgMatches};
use stackdriver_logging_utils::init_default_logger;

use settings::{key_or_default, key_or_none};

fn main() {
    // Help detect OpenSSL certificates on Alpine Linux
    openssl_probe::init_ssl_cert_env_vars();
    let matches = get_matches();

    let host = matches
        .value_of("host")
        .map(String::from)
        .unwrap_or(key_or_default("TECTONICDB_HOST", "0.0.0.0"));
    let port = matches
        .value_of("port")
        .map(String::from)
        .unwrap_or(key_or_default("TECTONICDB_PORT", "9001"));
    let dtf_folder = matches
        .value_of("dtf_folder")
        .map(String::from)
        .unwrap_or(key_or_default("TECTONICDB_DTF_FOLDER", "db"));
    let autoflush = {
        let cli_setting: bool = matches.is_present("autoflush");
        let env_setting = key_or_none("TECTONICDB_AUTOFLUSH");
        match env_setting {
            Some(s) => match s.as_ref() {
                "true" | "1" => true,
                "false" => false,
                _ => cli_setting,
            },
            None => cli_setting,
        }
    };
    let flush_interval = matches
        .value_of("flush_interval")
        .map(String::from)
        .unwrap_or(key_or_default("TECTONICDB_FLUSH_INTERVAL", "1000"));
    let hist_granularity = matches
        .value_of("hist_granularity")
        .map(String::from)
        .unwrap_or(key_or_default("TECTONICDB_HIST_GRANULARITY", "30"));
    let hist_q_capacity = matches
        .value_of("hist_q_capacity")
        .map(String::from)
        .unwrap_or(key_or_default("TECTONICDB_HIST_Q_CAPACITY", "300"));

    let settings = settings::Settings {
        autoflush: autoflush,
        dtf_folder: dtf_folder.to_owned(),
        flush_interval: flush_interval.parse().unwrap(),
        hist_granularity: hist_granularity.parse().unwrap(),
        hist_q_capacity: hist_q_capacity.parse().unwrap(),
    };

    init_default_logger();

    info!(r##"
           _/                            _/                          _/
        _/_/_/_/    _/_/      _/_/_/  _/_/_/_/    _/_/    _/_/_/          _/_/_/
         _/      _/_/_/_/  _/          _/      _/    _/  _/    _/  _/  _/
        _/      _/        _/          _/      _/    _/  _/    _/  _/  _/
         _/_/    _/_/_/    _/_/_/      _/_/    _/_/    _/    _/  _/    _/_/_/
    "##);

    server::run_server(&host, &port, &settings);
}

<<<<<<< HEAD
fn prepare_logger(verbosity: u8, log_file: &str) {
    let level = match verbosity {
        0 => log::LevelFilter::Error,
        1 => log::LevelFilter::Warn,
        2 => log::LevelFilter::Info,
        3 => log::LevelFilter::Debug,
        _ => log::LevelFilter::max(),
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S:%f]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(level)
        .level_for("tokio_core", log::LevelFilter::Info)
        .level_for("tokio_reactor", log::LevelFilter::Info)
        .level_for("hyper", log::LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file(log_file).unwrap())
        .apply()
        .unwrap();
}

=======
>>>>>>> ecca4d27f754d4f843520748532933619c68db6e
/// Gets configuration values from CLI arguments, falling back to environment variables
/// if they don't exist and to default values if neither exist.
fn get_matches<'a>() -> ArgMatches<'a> {
    App::new("tectonic-server")
        .version("1.0.0")
        .author("Ricky Han <tectonic@rickyhan.com>")
        .about("tectonic financial datastore")
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("HOST")
                .help("Sets the host to connect to (default 0.0.0.0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Sets the port to connect to (default 9001)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dtf_folder")
                .short("f")
                .long("dtf_folder")
                .value_name("FOLDER")
                .help("Sets the folder to serve dtf files")
                .takes_value(true),
        )
        .arg(Arg::with_name("autoflush").short("a").help(
            "Sets autoflush (default is false)",
        ))
        .arg(
            Arg::with_name("flush_interval")
                .short("i")
                .long("flush_interval")
                .value_name("INTERVAL")
                .help("Sets autoflush interval (default every 1000 inserts)"),
        )
        .arg(
            Arg::with_name("hist_granularity")
                .short("g")
                .long("hist_granularity")
                .value_name("HIST_GRANULARITY")
                .help(
                    "Sets the history record granularity interval. (default 60s)",
                ),
        )
        .get_matches()
}
