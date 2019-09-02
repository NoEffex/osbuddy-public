#!/usr/bin/env bash

source "env.sh"

docker run --rm --name "$REDIS_CONTAINER" -p 6379:6379 -d redis:5