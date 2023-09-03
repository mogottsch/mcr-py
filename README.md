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

## Running RAPTOR

1. Get GTFS data.

```
python src/main.py gtfs list --country-code FI
```

```
python src/main.py gtfs download 864 ./data/helsinki_gtfs.zip
```

2. (Optional) Crop GTFS data, by bounding box and time window

```
python src/main.py gtfs crop ./data/helsinki_gtfs.zip ./data/helsinki_gtfs_cropped.zip \
    --lat-min=60.150284 \
    --lat-max=60.24994 \
    --lon-min=24.850458 \
    --lon-max=24.99957 \
    --time-start 23.06.2023-00:00:00 \
    --time-end 24.06.2023-00:00:00
```

3. Clean GTFS data.

```
python src/main.py gtfs clean  ./data/helsinki_gtfs_cropped.zip ./data/cleaned/
```

3. Transform GTFS data into structures for efficient access.

```
python src/main.py build-structures ./data/cleaned/ ./data/structs.pkl
```

4. Find OSM dataset ID.
```
python src/main.py osm list --selector "cities"
```

5. Generate footpaths (WARNING: for Helsinki this might need ~20 GB of free RAM)

```
python src/main.py generate-footpaths \
    --output ./data/footpaths.pkl \
    --stops ./data/cleaned/stops.csv \
    --city-id Helsinki
```

6. Run RAPTOR

```
python src/main.py raptor \
    --footpaths ./data/footpaths.pkl \
    --structs ./data/structs.pkl \
    --start-stop-id 1040409 \
    --end-stop-id 1292101 \
    --start-time 15:00:00 \
    --output ./data/raptor_results.csv
```

6. Take a look at the results with the `results.ipynb` notebook.

### same with cologne

```
python src/main.py gtfs download 777 ./data/vrs.zip
python src/main.py gtfs crop ./data/vrs.zip ./data/cologne_gtfs.zip \
    --lat-min=50.888361 \
    --lat-max=50.988361 \
    --lon-min=6.889974 \
    --lon-max=6.999974 \
    --time-start 23.06.2023-00:00:00 \
    --time-end 24.06.2023-00:00:00
python src/main.py gtfs clean  ./data/cologne_gtfs.zip ./data/cleaned/
python src/main.py build-structures ./data/cleaned/ ./data/structs.pkl
python src/main.py generate-footpaths \
    --stops ./data/cleaned/stops.csv \
    --city-id Koeln \
    --output ./data/footpaths.pkl
python src/main.py raptor \
    --footpaths ./data/footpaths.pkl \
    --structs ./data/structs.pkl \
    --start-stop-id 818 \
    --end-stop-id 197 \
    --start-time 15:00:00 \
    --output-dir ./data/raptor_results.csv
```

### smaller area in cologne

```
python src/main.py gtfs crop ./data/vrs.zip ./data/cologne_gtfs.zip \
    --lat-max=50.981779 \
    --lat-min=50.936779 \
    --lon-max=6.978867 \
    --lon-min=6.908867000000001 \
    --time-start 23.06.2023-00:00:00 \
    --time-end 24.06.2023-00:00:00
```

### smallest area in cologne
```
python src/main.py gtfs crop ./data/vrs.zip ./data/cologne_gtfs.zip \
    --lat-max=50.981779 \
    --lat-min=50.973779 \
    --lon-max=6.973867 \
    --lon-min=6.9638670000000005 \
    --time-start 23.06.2023-00:00:00 \
    --time-end 24.06.2023-00:00:00
```

## Running MCR

```
python src/main.py gtfs download 777 ./data/vrs.zip
python src/main.py gtfs crop ./data/vrs.zip ./data/cologne_gtfs.zip \
    --lat-min=50.888361 \
    --lat-max=50.988361 \
    --lon-min=6.889974 \
    --lon-max=6.999974 \
    --time-start 23.06.2023-00:00:00 \
    --time-end 24.06.2023-00:00:00
python src/main.py gtfs clean  ./data/cologne_gtfs.zip ./data/cleaned/
python src/main.py build-structures ./data/cleaned/ ./data/structs.pkl
python src/main.py mcr --stops="data/cleaned/stops.csv" --city-id="Koeln" \
    --structs="data/structs.pkl" \
    --start-node-id=394001227 \
    --start-time=08:00:00 \
    --max-transfers=2 \
    --output=data/bags.pkl
```
