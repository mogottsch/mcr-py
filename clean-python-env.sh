#!/bin/bash

if [ -z "$CONDA_PREFIX" ]; then
    echo "No conda environment found. Exiting."
    exit 1
fi

# remove mcr_py* from $CONDA_PREFIX/lib/python3.10/site-packages/
rm -rf $CONDA_PREFIX/lib/python3.10/site-packages/mcr_py*

echo "Cleaned up mcr_py* from $CONDA_PREFIX/lib/python3.10/site-packages/"
