use std::error::Error;
use std::fs::{self, DirEntry};
use std::io;

use dtf::{self, Metadata};
use utils::within_range;

fn parse_dtf_entry(
    folder: &str,
    entry: io::Result<DirEntry>
) -> Result<Option<(String, Metadata)>, Box<Error>> {
    let entry = entry?;
    let fname = entry.file_name();
    let fname = match fname.to_str() {
        Some(fname) => fname,
        None => {
            error!("Invalid DTF entry detected: {:?}", entry);
            return Ok(None);
        }
    }.to_owned();
    let fname = format!("{}/{}", folder, fname);
    let meta = dtf::read_meta(&fname)?;

    Ok(Some((fname, meta)))
}

/// search every matching dtf file under folder
pub fn scan_files_for_range(
    folder: &str,
    symbol: &str,
    min_ts: u64,
    max_ts: u64,
) -> Result<Vec<dtf::Update>, io::Error> {
    let mut ret = Vec::new();
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ))
        },
        Ok(entries) => {
            let mut v = entries
                .map(|entry| {
                    match parse_dtf_entry(folder, entry) {
                        Ok(Some(res)) => Some(res),
                        Ok(None) => None,
                        Err(err) => {
                            error!("Error while processing DTF entry: {:?}", err);
                            None
                        }
                    }
                })
                .filter(|opt| {
                    match opt {
                        Some((ref _fname, ref meta)) => {
                            meta.symbol == symbol &&
                                within_range(min_ts, max_ts, meta.min_ts, meta.max_ts)
                        },
                        None => false,
                    }
                })
                .map(Option::unwrap)
                .collect::<Vec<_>>();

            // sort by min_ts
            v.sort_by(|&(ref _f0, ref m0), &(ref _f1, ref m1)| m0.cmp(m1));

            for &(ref fname, ref _meta) in v.iter() {
                let ups = dtf::get_range_in_file(fname, min_ts, max_ts)?;
                ret.extend(ups);
            }

        },
    };
    Ok(ret)
}

pub fn total_folder_updates_len(folder: &str) -> Result<usize, io::Error> {
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ))
        },
        Ok(entries) => {
            let mut count = entries
                .map(|entry| parse_dtf_entry(folder, entry))
                .fold(0, |acc, meta| {
                    match meta {
                        Ok(Some((_fname, meta))) => acc + meta.nums,
                        _ => acc,
                    }
                });

            Ok(count as usize)
        },
    }
}
