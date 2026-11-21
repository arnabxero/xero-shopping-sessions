#!/bin/bash
# cold_ab.sh VOLUME ROUNDS IMAGE...: drop the Docker VM page cache before every run (local Docker
# Desktop only); print each image's phase log on one line.
VOL="$1"; R="$2"; shift 2
export MSYS_NO_PATHCONV=1
for i in $(seq "$R"); do
  for img in "$@"; do
    docker run --rm -v shopout:/o debian:bookworm-slim sh -c "rm -rf /o/* ; chmod 777 /o"
    docker run --rm --privileged debian:bookworm-slim sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
    echo "$img: $(docker run --rm --network none --read-only --cpus 8 --memory 24g --user 1000:1000 -v "$VOL":/data:ro -v shopout:/output -e NCPU=8 $img 2>&1 | grep -E "setup|events/clicks|output written" | sed -E 's/^\[ *([0-9.]+) ms\] ([a-z/]+).*/\2 \1/' | tr '\n' ' ')"
  done
done
