#!/bin/bash
# ab_cpu.sh IMAGE_A IMAGE_B VOLUME ROUNDS: alternate runs, report internal wall ms and process CPU (user+sys) ms.
A="$1"; B="$2"; VOL="$3"; R="${4:-4}"
export MSYS_NO_PATHCONV=1
run() {
  docker run --rm -v shopout:/o debian:bookworm-slim sh -c "rm -rf /o/* ; chmod 777 /o"
  docker run --rm --network none --read-only --cpus 8 --memory 24g --user 1000:1000 \
    -v "$VOL":/data:ro -v shopout:/output -e NCPU=8 -e SHOPPING_DEBUG=1 "$1" 2>&1 \
    | awk '/output written/ {gsub(/[\[\]]/,""); w=$1} /cpu user/ {u=$3; s=$6} END {printf "wall %5.0f cpu %5.0f (u %5.0f s %4.0f)", w, u+s, u, s}'
}
for i in $(seq "$R"); do
  echo "A $(run "$A")  |  B $(run "$B")"
done
