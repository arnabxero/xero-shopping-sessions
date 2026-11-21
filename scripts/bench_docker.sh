#!/bin/bash
# Time the image on a dataset held in a docker volume: bench_docker.sh VOLUME [RUNS]
# Same scorer flags as docker_run.sh; the output goes to the volume "shopout".
set -u
VOL="$1"; RUNS="${2:-3}"
IMAGE="${IMAGE:-shopping-sessions:latest}"
export MSYS_NO_PATHCONV=1
docker volume create shopout >/dev/null
for i in $(seq "$RUNS"); do
  docker run --rm -v shopout:/o debian:bookworm-slim sh -c "rm -rf /o/* ; chmod 777 /o"
  s=$(date +%s.%N)
  docker run --rm --platform linux/amd64 --network none --read-only \
    --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL --security-opt no-new-privileges \
    --pids-limit 4096 --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 \
    -v "$VOL":/data:ro -v shopout:/output --tmpfs /scratch:rw,mode=1777 \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU=8 -e MEM_GB=24 "$IMAGE" 2> /tmp/bench_err.txt
  e=$(date +%s.%N)
  echo "run $i: wall $(awk "BEGIN{printf \"%.3f\", $e - $s}") s | $(tr '\n' '|' < /tmp/bench_err.txt)"
done
