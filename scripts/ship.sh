#!/bin/bash
# ship.sh TAG [COMMIT]: copy the sources (working tree, or COMMIT) to the alpine server
# and build image shop:TAG there (see /data/eftakhar/shopping/mkimg.sh).
set -e
T="$1"; C="${2:-}"
cd "$(dirname "$0")/.."
SSH="ssh -i $HOME/.ssh/alpine_builders -o BatchMode=yes -o IdentitiesOnly=yes -o LogLevel=ERROR eftakhar@alpine.cse.eng.auburn.edu"
if [ -n "$C" ]; then git archive --format=tar "$C" Cargo.toml Cargo.lock src .cargo; else tar cf - Cargo.toml Cargo.lock src .cargo; fi \
  | $SSH "cd /data/eftakhar/shopping && rm -rf src-$T && mkdir -p src-$T && tar xf - -C src-$T && ./mkimg.sh $T" 2>&1 | grep -v '^\*\*\*'
