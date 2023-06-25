# mcr-py

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

4. Generate footpaths (WARNING: for Helsinki this might need ~20 GB of free RAM)

```
python src/main.py generate-footpaths \
    --output ./data/footpaths.pkl \
    --stops ./data/cleaned/stops.csv \
    --city-id Helsinki
```

5. Run RAPTOR

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
