#!/usr/bin/env bash
cargo build --bin tectonic-server && target/debug/tectonic-server -vv -a -i 1000
