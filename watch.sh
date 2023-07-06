#!/bin/bash

# Run maturin develop when src/lib.rs changes
cargo watch -s "maturin develop"
