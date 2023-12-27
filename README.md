# mcr-py

## Installation

Recommendation: Use mamba to create the environment faster.

```
conda config --add channels conda-forge
conda config --set channel_priority strict
conda install mamba
```

Setup environment:

```
mamba env create -f environment.yaml
conda activate mcr-py
```

### Running Analysis for Cologne

```
python src/main.py gtfs download 777 ./data/vrs.zip
```
- Run `area.ipynb`
```
python src/main.py gtfs crop ./data/vrs.zip ./data/cologne_gtfs.zip \                                                                                        ─╯
    --geometa-path ./data/geometa.pkl \
    --time-start 23.06.2023-00:00:00 \
    --time-end 24.06.2023-00:00:00
python src/main.py gtfs clean  ./data/cologne_gtfs.zip ./data/cleaned/
python src/main.py build-structures ./data/cleaned/ ./data/structs.pkl
```
- Run `landuse.ipynb`
- Run `mcr5_input.ipynb`
- Run `mcr5.ipynb`
- Run `mcr5_results_calculation.ipynb`
- See `mcr5_results.ipynb`
