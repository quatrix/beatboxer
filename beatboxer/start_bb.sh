#!/bin/bash
#
export RUST_LOG=beatboxer=info,info
HTTP_PORT=$1
CLUSTER_PORT=$2
FIRST_RUN=1

echo "starting beatboxer http port ${HTTP_PORT} cluster port ${CLUSTER_PORT}"

cargo build --release --bin beatboxer || exit 
while true
do
    DELAY=$((RANDOM % 30))
    if [[ $FIRST_RUN == 0 ]]; then
        echo "waiting ${DELAY}..."
        sleep $DELAY
    fi
    target/release/beatboxer --listen-addr 0.0.0.0 --listen-port $CLUSTER_PORT -n Miedwars-MacBook-Pro-2.local:5500 -n Miedwars-MacBook-Pro-2.local:5501 -n Miedwars-MacBook-Pro-2.local:5502 --http-port $HTTP_PORT
    FIRST_RUN=0
done
