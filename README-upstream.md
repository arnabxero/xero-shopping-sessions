# shopping-sessions

Native Rust pipeline for the Shopping Sessions track. Released under the MIT License (see `LICENSE`). Third-party code: the `libc` crate only.

```sh
docker build -t shopping-sessions .
docker run --rm -v "$PWD/data:/data:ro" -v "$PWD/out:/output" shopping-sessions
```

The image reads `DATA_DIR` (default `/data`) and writes Parquet to `OUTPUT_DIR` (default `/output`).
