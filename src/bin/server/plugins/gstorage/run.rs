/// The uploader is run in a separate thread spawned from server thread,
///
/// The thread sleeps until next midnight,
/// then upload all the dtf files to google storage via REST endpoint
/// and once confirmed, delete local files.

use std::{fs::{self, DirEntry}, io, thread};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

extern crate rayon;
use self::rayon::prelude::*;
extern crate tempdir;
use self::tempdir::TempDir;
use uuid::Uuid;

use state::{SharedState, ThreadState};
use plugins::gstorage::GStorageConfig;
use plugins::gstorage::upload::{self, GStorageFile};

/// Posts a DTF file's metadata to the DCB, uploads it to Google Cloud Storage, and then
/// optionally deletes it after.
fn upload_file(path_buf: PathBuf, conf: &GStorageConfig) {
    let fname = match path_buf.to_str() {
        Some(p) => p,
        None => {
            error!("Unable to convert filename");
            return;
        }
    };

    let mut f = match GStorageFile::new(&conf, fname) {
        Ok(f) => f,
        Err(e) => {
            error!("fname: {}, {:?}", fname, e);
            return;
        },
    };

    match upload::upload(&mut f, fname) {
        Ok(metadata) => {
            debug!("DTF file {} successfully uploaded to google cloud storage.", fname);
            if let Some(ref dcb_url) = conf.dcb_url {
                match upload::post_to_dcb(&dcb_url, &metadata) {
                    Ok(res) => info!("DTF file metadata posted to the DCB: {:?}", res),
                    Err(err) => error!("Error while posting data to DCB: {:?}", err),
                }
            }

        }
        Err(e) => error!("fname: {}, {:?}", fname, e),
    };

    if conf.remove {
        match fs::remove_file(path_buf.as_path()) {
            Ok(_) => debug!("DTF file successfully deleted."),
            Err(err) => error!("Error while deleting DTF file: {:?}", err),
        }
    }
}

fn list_files<P: AsRef<Path>>(dir: P) -> Result<Vec<Result<DirEntry, io::Error>>, String> {
    Ok(match fs::read_dir(dir) {
        Ok(files) => files,
        Err(err) => {
            return Err(format!("Error while reading the files in the DTF directory: {:?}", err));
        },
    }.collect())
}

fn upload_all_files(dir_path: &Path) {
    let conf = GStorageConfig::new().unwrap();
    let files_to_upload: Vec<Result<DirEntry, io::Error>> = match list_files(dir_path) {
        Ok(files) => files,
        Err(err) => {
            error!("{}", err);
            return;
        },
    };

    // Upload all files in the directory
    files_to_upload.into_par_iter().for_each(|path_res| {
        match path_res {
            Ok(entry) => {
                // Upload the DTF file to Google Cloud Storage and post its metadata to
                // the DCB
                let file_path = entry.path();
                info!("Found file to upload: {:?}", file_path);
                upload_file(file_path, &conf);
            },
            Err(err) => error!("Error while reading dir entry: {:?}", err),
        }
    });
}

lazy_static! {
    static ref TMP_DIR: TempDir = tempdir::TempDir::new("tectonic")
        .expect("Unable to create temporary directory!");
}

/// Move all DTF files in the db directory to the temporary directory for uploading
fn copy_files(dtf_directory: &str, min_file_size_bytes: Option<u64>) {
    let files_to_copy: Vec<Result<DirEntry, io::Error>> = match list_files(dtf_directory) {
        Ok(files) => files,
        Err(err) => {
            error!("{}", err);
            return;
        },
    };

    files_to_copy.into_par_iter().for_each(|path_res| {
        match path_res {
            Ok(entry) => {
                let src_path = entry.path();
                let dtf_file_name = src_path.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap();
                let dtf_file_name = format!("{}-{}", Uuid::new_v4(), dtf_file_name);
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        error!("Error while fetching DTF metadata: {:?}", err);
                        return;
                    },
                };
                let file_size_bytes: u64 = metadata.len();

                if file_size_bytes >= min_file_size_bytes.unwrap_or(0) {
                    // move the file to the temporary directory to be uploaded
                    let dst_path = TMP_DIR.path().join(dtf_file_name);
                    match fs::rename(src_path.clone(), dst_path) {
                        Ok(_) => (),
                        Err(err) => error!(
                            "Error while moving DTF file for upload: {:?}",
                            err
                        ),
                    }
                }
            },
            Err(err) => error!("Error while reading dir entry: {:?}", err),
        }
    });
}

pub fn run(global: Arc<RwLock<SharedState>>) {
    let global_copy = global.clone();
    thread::spawn(move || {
        let conf = GStorageConfig::new().unwrap();
        let min_file_size_bytes = conf.min_file_size;
        info!("Initializing GStorage plugin with config: {:?}", conf);
        let dtf_directory = { global_copy.read().unwrap().settings.dtf_folder.clone() };

        loop {
            thread::sleep(Duration::from_secs(conf.upload_interval_secs));
            info!("Gstorage checking to see if any files need upload...");

            // Copy all files over the size threshhold into the temporary directory for uploading
            copy_files(&dtf_directory, Some(min_file_size_bytes));

            // Upload all files in the temporary directory
            upload_all_files(TMP_DIR.path());
        }
    });
}

/// Called when the database is being shut down.  Uploads all files, regardless of size.
pub fn run_exit_hook(state: &ThreadState<'static, 'static>) {
    let dtf_dir_path = { &state.global.read().unwrap().settings.dtf_folder.clone() };
    copy_files(&dtf_dir_path, None);
    upload_all_files(&TMP_DIR.path())
}
