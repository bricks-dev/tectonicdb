# Autoflusher Plugin

Periodically flushes all data to disk to save on memory usage.

## Configuration

There's one configuration option which is provided by an environment variable: `AUTOFLUSHER_INTERVAL` which defaults to 21600 (6 hours) if not supplied.  It takes a value in seconds, and every `interval` seconds all stores will be flushed.

This plugin is only enabled if `tectonic-server` is compiled with the `autoflusher` feature.
