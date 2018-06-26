//! Given multiple DTF files, combines the data within them and outputs a single
//! DTF file that contains the data from both of them after discarding any
//! duplicate updates.

extern crate clap;
extern crate libtectonic;
extern crate serde_json;

use std::collections::HashSet;
use std::process::exit;

use clap::{App, Arg};
use libtectonic::dtf::{self, Update};
use libtectonic::dtf::file_format::Metadata;

const USAGE: &'static str = "Usage: `dtfconcat2 input1 input2 input3 -o output`";
const DTF_ERROR: &'static str = "Unable to parse input DTF file!";


struct InputFile {
    pub filename: String,
    pub metadata: Metadata,
}

fn main() {
    let matches = App::new("dtfconcat2")
        .version("1.0.0")
        .about("Concatenates multiple DTF files into a single output file.
Examples:
    dtfconcat2 file1.dtf file2.dtf file3.dtf file4.dtf -o output.dtf
")
        .arg(
            Arg::with_name("input_files")
                .value_name("INPUT")
                .multiple(true)
                .help("First file to read")
                .required(true)
                .takes_value(true)
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .value_name("OUTPUT")
                .help("Output file")
                .required(true)
                .takes_value(true)
        )
        .arg(
            Arg::with_name("discontinuity_cutoff")
                .short("c")
                .value_name("DISCONTINUITY_CUTOFF")
                .help("Allowed gap between file updates in milliseconds")
                .default_value("0")
                .takes_value(true)
        )
        .get_matches();

    let filenames: Vec<&str> = matches
        .values_of("input_files")
        .unwrap()
        .collect();
    let output_filename = matches
        .value_of("output")
        .expect(USAGE);
    let discontinuity_cutoff: u64 = matches
        .value_of("discontinuity_cutoff")
        .unwrap()
        .parse()
        .expect(USAGE);

    if filenames.len() < 2 {
        println!("Please specify at least 2 input files.");
        exit(1);
    }

    // Get metadata for input files
    let mut input_files: Vec<InputFile> = filenames
        .iter()
        .map(|filename| InputFile {
            filename: (*filename).to_string(),
            metadata: dtf::read_meta(filename).expect(DTF_ERROR)
        })
        .collect();

    // Sanity checks to make sure they're all the same symbol
    let mut unique_symbols: Vec<String> = input_files
        .iter()
        .map(|f| f.metadata.symbol.to_string())
        .collect();
    unique_symbols.sort_unstable();
    unique_symbols.dedup();
    if unique_symbols.len() > 1 {
        println!(
            "ERROR: The input files provided have different symbols. Found {:?}",
            unique_symbols
        );
        exit(1);
    }

    input_files.sort_by_key(|f| f.metadata.min_ts);

    match combine_files(&input_files, output_filename, discontinuity_cutoff){
        Ok(()) => println!("Successfully merged {} files and output to {}",
                           input_files.len(), output_filename),
        Err(err) => {
            println!("{}", err);
            exit(1);
        }
    }

}

fn files_are_continuous(
    files: &Vec<InputFile>,
    discontinuity_cutoff: u64,
) -> bool {
    let mut prev = None;
    for current in files {
        if prev.is_none() {
            prev = Some(current);
            continue;
        }
        if prev.unwrap().metadata.max_ts + discontinuity_cutoff < current.metadata.min_ts {
            return false;
        }
        prev = Some(current);
    }
    true
}

fn combine_files(
    files: &Vec<InputFile>,
    output_filename: &str,
    discontinuity_cutoff: u64,
) -> Result<(), String> {

    // Check for file continuity
    if !files_are_continuous(files, discontinuity_cutoff) {
        return Err("ERROR: The provided input files are not continuous!".into());
    }

    let symbol = files[0].metadata.symbol.clone();

    let mut joined_updates: Vec<Update> = Vec::new();
    let mut previous_overlap_updates: Vec<Update> = Vec::new();
    let mut previous_max_ts: u64 = 0;

    for (index, current) in files.iter().enumerate() {

        let current_metadata = &current.metadata;

        let next_file_min_ts = if index + 1 < files.len() {
            files[index+1].metadata.min_ts
        } else {
            current_metadata.max_ts + 1
        };

        println!("{}", current.filename);
        let full_file = dtf::decode(&current.filename, None).map_err(|_| DTF_ERROR)?;

        // Get current file's non-overlapping updates
        let mut file_updates: Vec<Update> = full_file
            .iter()
            .filter(|&&Update { ts, .. }| {
                ts >= previous_max_ts + 1 && ts < next_file_min_ts
            })
            .cloned()
            .collect();
        // Get potentially overlapping updates at the start of the current file
        let mut current_overlap_updates: Vec<Update> = full_file
            .iter()
            .filter(|&&Update { ts, .. }| ts <= previous_max_ts)
            .cloned()
            .collect();
        println!("CURRENT_OVERLAP_UPDATES = {}", current_overlap_updates.len());

        // Compare current file's first updates with previous file's last updates
        // and remove any duplicates.
        previous_overlap_updates.append(&mut current_overlap_updates);
        let mut overlapping_updates: HashSet<String> = previous_overlap_updates
            .iter()
            .map(serde_json::to_string)
            .map(Result::unwrap)
            .collect();
        let mut overlapping_updates: Vec<Update> = overlapping_updates
            .drain()
            .map(|s| serde_json::from_str(&s).unwrap())
            .collect();
        overlapping_updates.sort();
        println!("OVERLAPPING_UPDATES = {}", overlapping_updates.len());

        // Append all (dedupped overlapping and regular) updates to output
        // vector
        joined_updates.append(&mut overlapping_updates);
        joined_updates.append(&mut file_updates);

        // Store updates at the end of the file to check for potential overlaps
        // on next iteration
        previous_overlap_updates = full_file
            .iter()
            .filter(|&&Update { ts, .. }| ts >= next_file_min_ts)
            .cloned()
            .collect();
        previous_max_ts = current_metadata.max_ts;
    }
    // In case there's any updates that weren't checked for overlaps (from the
    // last file we processed), add them at the end of the output vector.
    joined_updates.append(&mut previous_overlap_updates);

    // Write output file
    dtf::encode(output_filename, &symbol, &joined_updates)
        .map_err(|_| String::from("Error while writing output file!"))?;

    Ok(())
}

#[test]
fn dtf_merging() {
    use std::fs::remove_file;

    let mut update_timestamps_1: Vec<u64> = (0..1000).collect();
    update_timestamps_1.append(
        &mut vec![1001, 1002, 1003, 1004, 1004, 1007, 1008, 1009, 1009, 1010]
    );
    let update_timestamps_2: &[u64] = &[1008, 1009, 1009, 1010, 1010, 1011, 1012];

    let map_into_updates = |timestamps: &[u64], seq_offset: usize| -> Vec<Update> {
        let mut last_timestamp = 0;

        timestamps
            .into_iter()
            .enumerate()
            .map(|(i, ts)| {
                let update = Update {
                    ts: *ts,
                    seq: i as u32 + seq_offset as u32,
                    is_trade: false,
                    is_bid: true,
                    price: *ts as f32 + if last_timestamp == *ts { 1. } else { 0. },
                    size: *ts as f32,
                };

                last_timestamp = *ts;

                update
            })
            .collect()
    };

    // Generate test data
    let updates1 = map_into_updates(&update_timestamps_1, 0);
    let updates2 = map_into_updates(update_timestamps_2, 1006);

    // Write into DTF files
    let filename1 = "test/test-data/dtfconcat1.dtf";
    let filename2 = "test/test-data/dtfconcat2.dtf";
    let output_filename = "test/test-data/dtfconcat_out.dtf";

    dtf::encode(filename1, "test", &updates1).unwrap();
    dtf::encode(filename2, "test", &updates2).unwrap();

    let metadata1 = dtf::read_meta(filename1).unwrap();
    let metadata2 = dtf::read_meta(filename2).unwrap();

    let expected_ts_price: &[(u64, f32)] = &[
        (1001, 1001.),
        (1002, 1002.),
        (1003, 1003.),
        (1004, 1004.),
        (1004, 1005.),
        (1007, 1007.),
        (1008, 1008.),
        (1009, 1009.),
        (1009, 1010.),
        (1010, 1010.),
        (1010, 1011.),
        (1011, 1011.),
        (1012, 1012.),
    ];

    let input_files = vec![
        InputFile { filename: filename1.to_string(), metadata: metadata1 },
        InputFile { filename: filename2.to_string(), metadata: metadata2 },
    ];
    // Concat the files and verify that they contain the correct data
    combine_files(&input_files, output_filename, 0).unwrap();
    let merged_updates: Vec<Update> = dtf::decode(output_filename, None).unwrap();

    remove_file(filename1).unwrap();
    remove_file(filename2).unwrap();
    remove_file(output_filename).unwrap();

    let actual_ts_price: Vec<(u64, f32)> = merged_updates
        .into_iter()
        .skip(1000)
        .map(|Update { ts, price, .. }| (ts, price))
        .collect();

    assert_eq!(expected_ts_price, actual_ts_price.as_slice());
}
