# Linux toolchain image for the slow_reader spill benchmark matrix.
# Source is NOT copied in: run_matrix.sh mounts the worktree read-only at /src
# and builds into a named volume mounted at /build, so rebuilds are incremental.
#
# BASE defaults to a plain Ubuntu; run_matrix.sh passes realurl-linux:dev when it
# exists locally (already has clang-18/cmake/ninja/boost, the apt line below then
# only adds the missing -dev packages).
ARG BASE=ubuntu:24.04
FROM ${BASE}

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates clang-18 cmake ninja-build ccache \
        libc6-dev libstdc++-14-dev libboost-dev libssl-dev \
        libsqlite3-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CC=clang-18 CXX=clang++-18 \
    CCACHE_DIR=/ccache CCACHE_MAXSIZE=2G

COPY build_bench.sh run_row.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/build_bench.sh /usr/local/bin/run_row.sh

WORKDIR /build
CMD ["build_bench.sh"]
