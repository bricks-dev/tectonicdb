#[macro_use]

extern crate clap;
extern crate byteorder;
extern crate libtectonic;
use libtectonic::dtf;

use std::path::Path;
use std::process::exit;
use clap::{Arg, App};

fn main() {
    let matches = App::new("dtfsplit")
        .version("1.0.0")
        .author("Ricky Han <tectonic@rickyhan.com>")
        .about("Splits big dtf files into smaller ones
Examples:
    dtfsplit -i test.dtf -f test-{}.dtf
")
        .arg(
            Arg::with_name("input")
                .short("i")
                .long("input")
                .value_name("INPUT")
                .help("file to read")
                .required(true)
                .takes_value(true))
        .arg(
            Arg::with_name("BATCH")
                .short("b")
                .long("batch_size")
                .value_name("BATCH_SIZE")
                .help("Specify the number of batches to read")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("FILE_SIZE")
                .short("s")
                .long("file_size")
                .value_name("FILE_SIZE")
                .help("Specify the target file size")
                .required(false)
                .takes_value(true)
        )
        .get_matches();

    // single file
    let fname = matches.value_of("input").expect("Must supply input");
    let batch_size = value_t!(matches, "BATCH", u32).unwrap_or(0);
    let file_size = value_t!(matches, "FILE_SIZE", u64).unwrap_or(0);
    let file_stem = Path::new(fname).file_stem().expect("Input not a valid file").to_str().unwrap();

    if (batch_size == 0 && file_size == 0) || (batch_size > 0 && file_size > 0) {
        println!("Please provide only one of --batch-size or --file-size");
        exit(1);
    }

    println!("Reading: {}", fname);
    let meta = dtf::read_meta(fname).unwrap();
    let rdr = dtf::DTFBufReader::new(fname, batch_size);

    if batch_size > 0 {
        for (i, batch) in rdr.enumerate() {
            let outname = format!("{}-{}.dtf", file_stem, i);
            println!("Writing to {}", outname);
            dtf::encode(&outname, &meta.symbol, &batch).unwrap();
        }
        exit(0);
    }

    for (i, batch) in rdr.as_chunks(file_size).enumerate() {
        let outname = format!("{}-{}.dtf", file_stem, i);
        println!("Writing to {}", outname);
        dtf::encode(&outname, &meta.symbol, &batch).unwrap();
    }

}
