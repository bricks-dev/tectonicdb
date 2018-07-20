/// runner for plugins
// google cloud storage plugin
#[cfg(feature = "gcs")]
pub mod gstorage;
#[cfg(feature = "autoflusher")]
pub mod autoflusher;

// history plugin
pub mod history;

use std::sync::{Arc, RwLock};

use state::{SharedState, ThreadState};

/// Run each plugin in a separate thread
#[allow(unused_variables)]
pub fn run_plugins(global: Arc<RwLock<SharedState>>, threadstate: ThreadState<'static, 'static>) {
    history::run(global.clone());

    #[cfg(feature = "gcs")] gstorage::run(global.clone());
    #[cfg(feature = "autoflusher")] autoflusher::run(threadstate);
}

pub fn run_plugin_exit_hooks(state: &ThreadState<'static, 'static>) {
    #[cfg(feature = "gcs")] gstorage::run_exit_hook(state);
}

pub fn run_plugin_exit_hooks(state: &ThreadState<'static, 'static>) {
    #[cfg(feature = "gcs")] gstorage::run_exit_hook(state);
}
