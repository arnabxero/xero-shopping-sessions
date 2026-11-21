#!/bin/bash
# ab_docker.sh IMAGE_A IMAGE_B VOLUME ROUNDS: alternate A/B runs under scorer flags; print wall and internal ms.
A="$1"; B="$2"; VOL="$3"; R="${4:-4}"
export MSYS_NO_PATHCONV=1
run() {
  docker run --rm -v shopout:/o debian:bookworm-slim sh -c "rm -rf /o/* ; chmod 777 /o"
  local s=$(date +%s%N)
  local last=$(docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 --cpus 8 --memory 24g --memory-swap 24g \
    --user 1000:1000 -v "$VOL":/data:ro -v shopout:/output -e NCPU=8 "$1" 2>&1 | tail -1 | sed -E 's/^\[ *([0-9.]+) ms.*/\1/')
  local e=$(date +%s%N)
  echo "$(( (e - s) / 1000000 )) $last"
}
for i in $(seq "$R"); do
  echo "A $(run "$A")   B $(run "$B")"
done
