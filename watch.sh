#!/bin/bash

# check CONDA_PREFIX is set, if not activate 'mcr-py'
if [ -z "$CONDA_PREFIX" ]; then
    eval "$(conda shell.bash hook)"
    echo "CONDA_PREFIX not set, activating 'mcr-py'"
    conda activate mcr-py
fi


# Run maturin develop when src/lib.rs changes
cargo watch -s "maturin develop"
