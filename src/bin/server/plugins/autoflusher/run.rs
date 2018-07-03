use std::env;
use std::thread;
use std::time::Duration;

use state::ThreadState;

lazy_static! {
    static ref SLEEP_TIMEOUT: u64 = env::var("AUTOFLUSHER_INTERVAL")
        .unwrap_or("21600".into())
        .parse()
        .unwrap_or_else(|err| {
            error!("Error parsing supplied value for `AUTOFLUSHER_INTERVAL`: {}", err);
            21600
        });
}

pub fn run(mut threadstate: ThreadState<'static, 'static>) {
    thread::spawn(move || {
        let sleep_dur = Duration::from_secs(*SLEEP_TIMEOUT);
        loop {
            thread::sleep(sleep_dur);
            info!("[AUTOFLUSHER] Flushing all stores to disk...");
            threadstate.flushall();
            info!("[AUTOFLUSHER] All stores flushed.");
        }
    });
}
